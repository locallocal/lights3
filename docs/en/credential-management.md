# Credential Management: AK/SK Generation, Lookup, and Persistence (Design)

> English translation of [../credential-management.md](../credential-management.md). The Chinese original is authoritative; section numbering matches.

> Status: both phase 1 and phase 2 are implemented (phasing in §9, phase-2 design in §10). Builds on the `ICredentialProvider` extension point reserved in docs/s3-protocol.md §3.5.
> Code: `src/s3/auth/credential_store.{h,cc}`, `src/s3/handlers/admin_credentials.cc`.

## 1. Goals and Non-Goals

**Goals**

- Generate / query / revoke AK/SK at runtime through an API, with no config
  file change or process restart;
- Generated credentials are persisted via `IStorageBackend` and automatically
  recovered after a process restart;
- The signature-verification path keeps its current synchronous in-memory
  lookup; dynamic credentials introduce no asynchrony or noticeable overhead;
- The behavior of static credentials in the config file is entirely unchanged
  (backward compatible).

**Non-goals (not in phase 1; items marked have since been filled in by phase 2,
see §10)**

- IAM-style fine-grained policy — all credentials are still equivalent to
  superuser data-plane permissions (see §3, the two-level model).
  Phase 2 added a lightweight per-credential policy (bucket / key prefix /
  action, §10.4), still not IAM;
- STS temporary credentials / credential rotation expiry;
- Cross-node invalidation notifications when multiple instances share a backend
  (limitation in §7) — phase 2 fills this in with periodic incremental reload
  (§10.3).

## 2. API Design

Reuses the `/-/` reserved path (precedent: the existing `/-/healthz`,
`/-/metrics`), mounted under `/-/admin/credentials`. Unlike the anonymous
endpoints such as `/-/healthz`, the admin API **must pass SigV4 verification
and the caller must be a root credential** (defined in §3).

| Method and path | Operation | Success response |
| --- | --- | --- |
| `POST /-/admin/credentials` | Generate an AK/SK pair, optional `?comment=` note | `201` + JSON (the one and only full return of the SK) |
| `GET /-/admin/credentials` | List all credentials (including static ones, SK masked) | `200` + JSON list |
| `GET /-/admin/credentials/{ak}` | Query a single credential's metadata; `?show-secret=true` returns the plaintext SK (**dynamic/file credentials only** — static ones stay masked, see §10.5) | `200` + JSON |
| `DELETE /-/admin/credentials/{ak}` | Revoke (dynamic credentials only; static credentials belong to the config file) | `204` |

Companion ops CLI: `s3adm` (`src/tools/s3adm.cc`, built next to `lights3`,
subcommand framework `third_party/ccmd`). Credential operations live in the
`cred` command group; its four subcommands `cred list` / `cred get <ak>` /
`cred create` / `cred delete <ak>` map one-to-one onto the table above and
sign with SigV4 themselves. The root AK/SK come from `--ak=`/`--sk=` or the
env vars `LIGHTS3_ADMIN_AK`/`LIGHTS3_ADMIN_SK` (prefer env for the SK: argv is
visible to local `ps`); options must follow the leaf subcommand and long
options take values as `--name=value` (ccmd semantics); `cred get` supports
`--show-secret`, `cred create` supports `--comment` and `--policy` (inline
JSON or `@file`). Full reference: [cli.md §3.2](cli.md), or
`s3adm help cred [command]`.

Responses use JSON; serialization/parsing brings in
[nlohmann/json](https://github.com/nlohmann/json) (header-only, git submodule
under `third_party/`, managed the same way as ccmd/spdlog/httplib; the
integration is in §5.4). The admin plane is a newly minted API with no S3
compatibility baggage, and JSON is friendlier to both humans and scripts; the
data-plane S3 protocol keeps going through `s3/xml.cc`, and the two never
interfere. Examples:

```json
// POST response (201)
{
  "access_key": "L3AK7Q2MXX5EIY4BJZW3",
  "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY0",
  "comment": "ci-runner",
  "created_at": "2026-07-17T12:00:00Z"
}

// GET list response (200; SK masked, source distinguishes static/dynamic)
{
  "credentials": [
    { "access_key": "AKIDEXAMPLE", "secret_key_masked": "wJal****KEY0",
      "source": "static" },
    { "access_key": "L3AK7Q2MXX5EIY4BJZW3", "secret_key_masked": "wJal****KEY0",
      "source": "dynamic", "comment": "ci-runner",
      "created_at": "2026-07-17T12:00:00Z" }
  ]
}
```

Error codes reuse the existing `S3Error` system, but the admin branch catches
them itself and renders a JSON body
`{"code": "AccessDenied", "message": "..."}` (HTTP status unchanged); only
unexpected exceptions that leak out to dispatch's outer catch-all fall back to
the S3 XML 500. Mapping:

| Scenario | Error |
| --- | --- |
| Non-root credential calls the admin API | `AccessDenied` (403) |
| Authentication disabled overall (no static credentials) | `AccessDenied` — otherwise anyone could mint credentials, see §3 |
| The queried/deleted AK does not exist | `InvalidAccessKeyId` (403, consistent with the verification path) |
| Deleting a static credential | `MethodNotAllowed` (405) |
| AK collision on generation, retries still failing | `InternalError` (500) |

**Design trade-off — should queries return the SK**: masked by default,
`?show-secret=true` to ask explicitly. SigV4 is an HMAC scheme, so the server
must store the SK reversibly (it cannot store only a hash); therefore
"queries return plaintext" is unavoidable capability-wise — but masking by
default eliminates the cheap leak surfaces: list pages, logs, terminal echoes.
**Exception: static (root) credentials stay masked unconditionally** — they
come from config/environment, their holder already has the original, and
echoing them through the admin API only adds a leak surface; the
"unavoidable" argument does not apply to them (§10.5).

## 3. Permission Model: Two-Level Credentials

```text
Static credentials (config auth.credentials)      = root:    data plane + admin API
Dynamic credentials (API-generated, storage-persisted) = normal: data plane only
```

- Dynamic credentials cannot in turn call the admin API, cutting off the
  "credentials minting credentials" privilege-escalation chain;
- When authentication is disabled (the static table is empty), the admin API is
  rejected as well: no root, no admin plane;
- AK provenance is determined inside `CredentialStore` itself (the two sources,
  static/dynamic, are tagged).

## 4. Storage Layout

### 4.1 Location: the reserved system bucket `.sys`

- Credentials are written to a bucket named `.sys` on the `default_backend`,
  object key `credentials/{ak}` — one object per credential;
- `validate_bucket_name()` is also called inside each backend, so it **admits**
  the reserved name `.sys` (src/storage/validate.cc); interception of user
  requests is lifted to L2: dispatch rejects any bucket starting with `.`
  before routing (InvalidBucketName), so `.sys` is reachable only by
  CredentialStore; `ListBuckets` aggregation filters the `.` prefix likewise;
- Before the first write, `create_bucket(".sys")` creates the bucket lazily
  (idempotent; ignored if it already exists).

Why `IStorageBackend` instead of a side-channel local file: it reuses the
ready-made atomic write (LocalFs staging + rename) and follows automatically
when the backend changes; the cost is that credentials are not persistent under
the memory backend — stating this limitation in the docs and the startup log is
enough (memory is a test backend anyway).

### 4.2 Object Contents

The admin plane already brings in nlohmann/json (§2); the on-disk format is
JSON too, sharing one set of serialization code between reads and writes:

```json
{
  "version": 1,
  "sk": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY0",
  "created": "2026-07-17T12:00:00Z",
  "comment": "ci-runner"
}
```

Without a master key set, the SK lands in plaintext (version=1), at the same
confidentiality level as the static SK in the config file (same machine, same
file permissions). With `LIGHTS3_MASTER_KEY` set, it lands as the version=2
AES-256-GCM encrypted format, and existing v1 objects are upgraded in place at
startup (§10.1).

## 5. Components and Data Flow

### 5.1 New `s3/auth/credential_store.{h,cc}` (L2)

```cpp
class CredentialStore final : public ICredentialProvider {
public:
    // Full load at startup: list(".sys", "credentials/") + get one by one,
    // merged with the static table (on a duplicate AK the static one wins, with
    // a warning). Since phase 2 it takes the whole AuthConfig (including the
    // credentials_file / credentials_file_reload / sync_interval settings)
    static Task<std::shared_ptr<CredentialStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& cfg);

    // Verification hot path: synchronous in-memory lookup (shared_mutex read lock)
    std::optional<std::string> secret_for(std::string_view ak) const override;
    bool has_credentials() const override;
    bool is_root(std::string_view ak) const;   // static source → root

    // Admin plane: write storage first, then update memory (write-through; on a
    // crash, storage is authoritative — memory can at most have "less", never
    // "more"). Note: no co_await while holding a lock (the coroutine may resume
    // on another thread, and unlocking a std::mutex across threads is UB);
    // uniqueness under concurrency relies on the AK random space.
    // policy is a phase-2 extension (§10.4); default is no policy.
    Task<CredentialInfo> generate(std::string comment,
                                  std::optional<CredentialPolicy> policy = std::nullopt);
    Task<void>           remove(std::string_view ak);
    std::optional<CredentialInfo> find(std::string_view ak) const;
    std::vector<CredentialInfo>   list() const;
};
```

### 5.2 `SigV4Authenticator` Rework

Replace the private `std::map creds_` with the interface reserved in
docs/s3-protocol.md §3.5:

```cpp
struct ICredentialProvider {
    virtual std::optional<std::string> secret_for(std::string_view ak) const = 0;
    virtual bool has_credentials() const = 0;  // enabled(), made dynamic
};
```

- The table lookup inside `verify()` becomes `provider_->secret_for(ak)`; all
  other logic is untouched;
- The existing `build(AuthConfig)` remains: it wraps the static table into a
  pure in-memory provider internally, so unit tests and deployments that don't
  enable the credential API are entirely unaffected;
- `CredentialStore` implements this interface and is injected during main's
  assembly.

### 5.3 Request Flow (generation as the example)

```text
POST /-/admin/credentials?comment=ci
  → dispatch: path prefix /-/admin/ → auth_.verify(req) (reuses existing verification)
  → store.is_root(ak) fails → AccessDenied
  → store.generate("ci")
      ├─ CSPRNG generates AK/SK (§6), in-memory dedup, retry on collision (≤3 times)
      ├─ put_object(".sys", "credentials/{ak}", JSON)      // persist first
      └─ update the in-memory map under the write lock      // takes effect after
  → 201 + JSON (contains the plaintext SK)
```

The `/-/admin/` branch in dispatch is inserted after the existing anonymous
`/-/` endpoints and before S3 addressing (the dispatch in src/s3/service.cc
already has that if-else chain).

### 5.4 Introducing the nlohmann/json Dependency

- git submodule: `third_party/json` (header-only, no build artifacts);
- CMake: `add_subdirectory(third_party/json EXCLUDE_FROM_ALL)` then
  `target_link_libraries(lights3_core PRIVATE nlohmann_json::nlohmann_json)`,
  consistent with how ccmd/spdlog are wired in;
- build.sh's regular submodule list (`LIGHT_MODULES`) gains one entry;
- The usage surface is confined to the admin handler and `CredentialStore`'s
  serialization; it does not leak into L1/L3/L4 headers
  (`#include <nlohmann/json.hpp>` appears only in .cc files).

## 6. AK/SK Generation

- **AK**: `L3AK` prefix + 16 base32 chars (`A-Z2-7`), 20 characters total —
  length and character set aligned with AWS's `AKIA…` shape, so any client
  that validates input by AWS rules will accept it; the prefix makes dynamic
  credentials recognizable at a glance in logs;
- **SK**: 30 random bytes base64-encoded into 40 characters, matching the AWS
  SK length;
- The random source is uniformly `getentropy(2)` (a CSPRNG, no seed-management
  issues); `std::mt19937/rand` are **forbidden**;
- Uniqueness: in-memory map dedup suffices (the single-process write path is
  already serialized); collision probability is on the order of 2^-80, and the
  retry is purely defensive.

## 7. Concurrency and Consistency

- Reads (verification lookups): `shared_mutex` read lock; the hot path has no
  blocking write contention;
- Writes (generate/revoke): no lock held across `co_await` (the coroutine may
  resume on another thread, and unlocking a `std::mutex` across threads is UB);
  instead, "finish the storage write, then briefly take the write lock to
  update memory"; uniqueness of concurrent generates is guaranteed by the AK
  random space (2^80);
- Revocation semantics: after deletion, **new requests** fail immediately;
  requests that already passed verification and are still in flight complete
  under the **policy snapshot taken at verification time** (carried by
  `VerifiedIdentity`, never re-queried from the store — a re-query landing in
  the race window would make constraints like readonly vanish wholesale; same
  as AWS's eventually-consistent behavior);
- Multi-instance limitation: when multiple gateway instances share the same
  backend, there is no invalidation/addition notification between instances;
  each in-memory table is loaded only at startup by default. Phase 2 provides
  `auth.sync_interval` periodic incremental reload (§10.3); when not enabled,
  the single-process assumption stands (the deployment model of
  docs/architecture.md).

## 8. Test Plan

**Unit tests** (tests/unit, using the existing test framework)

- CredentialStore: generate → lookup hit; revoke → lookup miss; after writing
  under the memory backend, a freshly constructed store can re-load and recover
  (simulating a restart); on a static/dynamic AK conflict the static one wins;
- SigV4 integration: sign with a generated credential
  (`SigV4Authenticator::sign` is ready-made) then verify — the full chain
  passes;
- Permissions: dynamic credential calling the admin API → AccessDenied; with
  authentication disabled → AccessDenied;
- Edge cases: deleting a static credential is 405, querying a nonexistent AK,
  AK/SK character-set and length assertions.

**e2e** (one more section appended to tests/e2e/run_e2e.sh)

```text
root credential POST generate → parse the response JSON to extract the new AK/SK
  (sed/grep field extraction is enough; do not add a jq dependency to the e2e script)
  → PUT/GET an object with the new credential (curl --aws-sigv4)
  → GET the list, confirm it exists and the SK is masked
  → DELETE to revoke → request again with the new credential → 403 InvalidAccessKeyId
  → restart the server → another generated credential still works
    (persistence verification, localfs backend)
```

## 9. Phasing

| Phase | Contents | Status |
| --- | --- | --- |
| Phase 1 | All of this design: the 4 APIs, `.sys` persistence, dynamic effect, two-level permissions, unit tests + e2e | Implemented |
| Phase 2 | SK at-rest encryption (master key), file hot-reload provider, multi-instance invalidation sync, per-credential policy (design in §10) | Implemented |

## 10. Phase-2 Design

After phase 1, the credential sources expand from two levels to three sources:

```text
Static credentials (config auth.credentials)          = root:   data plane + admin API
File credentials (auth.credentials_file, hot reload)  = normal: data plane only, may carry a policy
Dynamic credentials (API-generated, storage-persisted) = normal: data plane only, may carry a policy
```

Duplicate-AK priority is static > file > dynamic, each with a startup/load
warning. `CredentialInfo::source` is a three-value enum replacing phase 1's
`is_static` boolean (`is_static()` is kept as a convenience method based on
source); the admin API's `source` field correspondingly gains `"file"`.

### 10.1 SK At-Rest Encryption

- The switch is the environment variable `LIGHTS3_MASTER_KEY` (64 hex
  characters = 32 bytes, generated with `openssl rand -hex 32`); it does not go
  into the config file, avoiding co-location with the material it encrypts;
- Algorithm: AES-256-GCM (OpenSSL EVP, wrapped in `core/util/crypto.h`:
  `aes256gcm_seal/open`, layout `12B nonce || ciphertext || 16B tag`);
- The on-disk object becomes `version: 2`, with the `sk` field replaced by
  `sk_enc` (the seal output in hex);
- Compatibility and upgrade: load accepts both v1/v2; once the master key is
  set, existing v1 objects are **rewritten in place as v2** at startup (the
  object count is bounded, the one-time cost is negligible);
- Fail-fast: encountering a v2 object with the key unset / wrong key (GCM tag
  verification failure) is a **startup error** rather than a skip — silently
  dropping credentials would lock users out with nothing to debug; corrupted
  JSON is still skip + warn (as in phase 1). During runtime sync (§10.3) the
  same class of errors is downgraded to a warning, so a single bad object does
  not break the sync;
- No downgrade path: to retire the master key, export via `?show-secret=true`
  first and recreate the credentials.

### 10.2 External Credentials File Hot-Reload Provider

For the scenario "credentials are generated and distributed by an external
system (IdP/config management)": lights3 only consumes the file and does no
protocol integration — the external system is responsible for rendering
credentials into a JSON file at the given path.

- Config `auth.credentials_file`, format:
  `{"credentials": [{"access_key", "secret_key", "comment"?, "policy"?}]}`;
- Hot reload: `auth.credentials_file_reload` (default 30s, 0s = load at startup
  only) polls the mtime periodically; on change, the file-source entries are
  replaced wholesale (credentials removed from the file lose validity with it).
  mtime polling was chosen over inotify: reliable across filesystems, less
  code, and 30s-level latency is plenty for credential distribution;
- A parse failure at startup is fail-fast (config error); a reload failure at
  runtime warns and **keeps the old table** (better for old credentials to live
  one extra round than for a parse error to wipe the whole table);
- File credentials are data-plane only (cannot call the admin API), nor can
  they be revoked via the admin API (405; they belong to the file — deleting
  from the file is the revocation).

### 10.3 Multi-Instance Invalidation Sync (periodic incremental reload)

There were two candidates for filling in §7's multi-instance limitation:
admin-plane broadcast (instances exchanging invalidation/addition events) and
periodic incremental reload. **Periodic incremental reload** was chosen: no new
admin-plane broadcast channel or membership discovery needed, at the cost of
invalidation being delayed by one period (credential distribution/revocation is
a minute-scale operational action anyway).

- Config `auth.sync_interval` (default 0s = off; zero overhead for
  single-instance deployments);
- Each round: first take a snapshot of the in-memory dynamic credentials' AKs,
  then list `.sys/credentials/` — entries in storage but not in memory are
  fetched and added (additions), entries in the snapshot but not in storage are
  removed (revocations). The snapshot **must precede the list**: write-through
  guarantees that credentials in the snapshot were already persisted at that
  moment, so "in snapshot + not in list" can only mean revoked elsewhere;
  credentials generated by this instance during the list are not in the
  snapshot and cannot be removed by mistake;
- Existing AKs are not re-fetched: SK and policy are immutable within a
  credential's lifetime (no update API), so the increments are only additions
  and removals;
- The timer pattern is the same as duostore GC (`BackgroundTaskGroup` +
  `TimerQueue`, re-armed after completion, no overlap); a tick first
  `pool_->schedule()`s onto a pool thread before doing IO.

### 10.4 Per-Credential Policy

Deliberately kept at the "good enough" tier — no IAM statement/effect/condition
syntax — but with three dimensions: bucket, key prefix and action
(docs/archive/gaps.md §5.10):

```json
{ "policy": { "buckets": ["logs-*", "backup"], "prefixes": ["tenant-a/"],
              "actions": ["read", "write"] } }
```

- `buckets`: bucket glob whitelist (fnmatch **with FNM_PATHNAME**, so `*` does
  not cross `/`); empty/absent = all;
- `prefixes`: key prefix whitelist; empty/absent = all. With it, multi-tenant
  shared buckets no longer degrade into "one bucket per tenant". It only applies
  to operations tied to a specific object — bucket-level operations such as
  creating a bucket or listing its objects are not tied to a key and are not
  restricted by prefixes;
- `actions`: whitelist of `read` / `write` / `delete`; when empty/absent it
  falls back to `readonly`. Actions are classified by **consequence**, not HTTP
  method: `DeleteObjects` is a POST but counts as `delete`, while
  `CreateMultipartUpload` is also a POST yet counts as `write` — the method
  dimension cannot separate the two. This dimension covers the most common
  backup case: may write, may not delete;
- `readonly`: equivalent to `actions: ["read"]`, kept for compatibility; when
  both appear, `actions` wins;
- How it is carried: the JSON body of `POST /-/admin/credentials`
  `{"comment"?, "policy"?}` (`?comment=` as a query parameter stays compatible,
  body wins), or the `policy` field of a credentials_file entry; immutable
  after creation (no update API — recreate instead);
- Enforcement point: after dispatch passes verification and resolves the
  bucket, the action of the **matched route** is checked against the policy
  snapshot taken at verification time (`src/s3/service.cc`, see §3.7). Static
  credentials and credentials without a policy always pass; denial is
  `AccessDenied` (403) and surfaces before data-plane errors such as
  NoSuchBucket. When no route matches (unsupported method) there is no action to
  judge, so the request goes straight to 405;
- CopyObject / UploadPartCopy carry their source in a header, bypassing the path
  check above: the **source bucket + source key** get their own `read`
  authorization, so a restricted credential cannot use a copy to read data
  outside its whitelist;
- Strict validation: an unknown field or unknown action name in the POST body /
  policy is an outright `InvalidRequest` — a misspelled restriction field
  silently ignored would amount to granting permission;
- `ListBuckets` results are **filtered by policy**: bucket names are themselves
  the first step of an attack chain, and a restricted credential should not learn
  that buckets outside its whitelist exist;
- Known trade-off: neither revocation nor policy affects in-flight requests that
  already passed verification (the §7 semantics).

### 10.5 Static Credential Secrets Are Never Returned by the Admin API

`?show-secret=true` applies only to dynamic and file credentials; static (root)
credentials always come back masked (docs/archive/gaps.md §5.10). The reason is the trust
boundary: a static SK comes from the config file or environment, so being able to
retrieve it downgrades "can read the config file" to "can send one HTTP GET" —
and a root SK is precisely the one that **cannot** be revoked through the admin
API (`DELETE` refuses static credentials), so a leak can only be resolved by
editing the config and restarting. The mask was also tightened from
"first 4 + last 4" to just the first 4 characters — an operator-chosen SK does
not necessarily have much entropy, and leaking both ends is needless. Every
`?show-secret=true` request is recorded in a WARN audit log, whether or not a
secret is returned.
