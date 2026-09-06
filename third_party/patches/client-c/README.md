# client-c patches pending upstream

Proposed changes to `tikv/client-c` that this repository's TiKV sidecar
(`src/storage/duostore/tikv_client.cc`) is already written against. The
submodule pointer stays on upstream (`@78a557e`) until they merge; the sidecar
detects at compile time whether the linked library carries them
(`is_upstream_write_conflict`) and falls back to the `@78a557e` behaviour
otherwise, so the build is correct with or without the patch applied.

| Patch | Upstream PR | Status |
| --- | --- | --- |
| `0001-LockResolver-throw-a-structured-WriteConflict-error-.patch` | to be opened against `tikv/client-c` master (title: *LockResolver: throw a structured WriteConflict error code*) | pending |

## Apply locally (verification builds)

```bash
git -C third_party/client-c am ../patches/client-c/0001-*.patch
cmake --build build-tikv -j$(( $(nproc) / 2 )) --target unit_tests
# revert to the pinned pointer afterwards (never commit the patched submodule):
git -C third_party/client-c checkout --detach 78a557e
```

## After the upstream merge

1. Bump the submodule pointer to the merged commit.
2. `tests/unit/test_duostore_tikv.cc: duostore_tikv_write_conflict_classification`
   flips to the coded branch on its own; run the conflict cases against a
   real cluster (`LIGHTS3_TEST_PD_ADDR`, tiup playground).
3. Delete the message-string branch of `is_upstream_write_conflict` and the
   detection template; delete this directory's patch and the row above.

## PR description (paste into the upstream PR)

`resolveLocksForWrite` aborts the caller when a key is held by a live optimistic
transaction newer than the caller's `start_ts`, but the exception carried
`ErrorCodes::UnknownError` (the `TODO` next to it). A caller that wants to retry
with a fresh `start_ts` in exactly that case had to match the `"write conflict"`
message string.

- add `ErrorCodes::WriteConflict` (appended; existing values unchanged);
- throw it from that site, with the lock's debug string and both timestamps in
  the message;
- the two bare `Exception` throws in `RegionClient.h` (TiFlash `label_filter`
  misuse) get `LogicalError`, so every exception the client raises carries a
  code (`RegionCache` already throws `RegionUnavailable`; `Backoffer` rethrows
  the caller's exception and thus its code).

No behaviour change for callers that do not inspect the code. Not covered by
`src/test` (the mock-tikv failpoints have no "newer live lock" scenario); the
downstream user exercises the path against a real cluster.
