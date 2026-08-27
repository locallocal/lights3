// L2/L3 boundary: storage backend interface (see docs/storage-backend.md)
// Error convention: backends throw s3::S3Error and are unaware of HTTP.
#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/task.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::storage {

// The three forms of an HTTP Range: a-b(first,last) / a-(first) / -n(last only, suffix of n bytes)
struct ByteRange {
    std::optional<uint64_t> first;
    std::optional<uint64_t> last;
};

// Resolve against the object size into a closed interval [first,last]; throws InvalidRange(416) if unsatisfiable
std::pair<uint64_t, uint64_t> resolve_range(const ByteRange& r, uint64_t size);

struct ObjectMeta {
    std::string key;
    uint64_t size = 0;
    std::string etag;  // hex without quotes
    std::string content_type = "binary/octet-stream";
    std::chrono::system_clock::time_point last_modified;
    std::map<std::string, std::string> user_meta;  // x-amz-meta-* kv pairs with the prefix stripped

    // First-class S3 object metadata (docs/archive/gaps.md §5.2): previously all dropped on PUT and
    // never returned on GET. Dropping content_encoding=gzip leaves browsers with a byte
    // stream they cannot decompress -- these are not "extra user metadata" but part of
    // content negotiation. Empty string = unset, header is not returned
    std::string cache_control;
    std::string content_disposition;
    std::string content_encoding;
    std::string content_language;
    std::string expires;  // HTTP-date text stored verbatim
    // Website redirect (docs/static-website.md phase ③): echoed as a header on GET/HEAD
    // like the fields above; the anonymous website plane additionally answers 301 with
    // it as Location. Value must start with '/', 'http://' or 'https://' (checked at PUT)
    std::string website_redirect;
    // Note: x-amz-storage-class is deliberately absent. This implementation has only the
    // STANDARD storage class; storing the client-reported value and echoing it back would
    // make the storage layer lie (the object sits on local disk yet reports GLACIER);
    // non-STANDARD gets a direct 501 at L2, consistent with the handling of x-amz-acl
    // (docs/archive/gaps.md §5.2)

    // Checksum closure (roadmap §2.2): the client-declared, gateway-verified checksum
    // persists with the object and is echoed on GET/HEAD under x-amz-checksum-mode.
    // algorithm is the uppercase wire name (CRC32/CRC32C/CRC64NVME/SHA1/SHA256), value
    // is base64 (composite values carry the "-N" suffix), type is FULL_OBJECT or
    // COMPOSITE. All empty = no checksum recorded
    std::string checksum_algorithm;
    std::string checksum_value;
    std::string checksum_type;
    // Trailer-form uploads (STREAMING-*-TRAILER) declare the checksum after the payload:
    // the L2 capture decorator fills this slot when the body reaches EOF — which the
    // put_object contract guarantees happens before the backend commits. Serializers
    // read through resolved_checksum_value(); never persisted itself
    std::shared_ptr<const std::string> checksum_pending;
    // Multipart part layout recorded at complete (roadmap §2.5 GET ?partNumber):
    // part_sizes[i] is the size of part i+1. Empty = single-part or unknown (objects
    // completed before this field existed)
    std::vector<uint64_t> part_sizes;

    // Object tagging (roadmap §2.5): canonical URL-encoded "k=v&k2=v2" (both sides
    // aws_uri_encoded — safe for every text serializer), empty = no tags. Rides
    // kStdMetaFields for extraction/persistence but is never echoed as a header
    // (GET answers x-amz-tagging-count instead; ?tagging returns the XML)
    std::string tagging;
};

// The value a serializer must persist: header-form checksums sit in checksum_value from
// the start; trailer-form values land in checksum_pending once the body is drained
inline std::string resolved_checksum_value(const ObjectMeta& m) {
    if (!m.checksum_value.empty()) return m.checksum_value;
    return m.checksum_pending ? *m.checksum_pending : std::string();
}
// In-place variant for backends that keep the ObjectMeta struct as-is (memory backend)
inline void finalize_checksum(ObjectMeta& m) {
    if (m.checksum_value.empty() && m.checksum_pending) m.checksum_value = *m.checksum_pending;
    m.checksum_pending.reset();
}

// part_sizes wire form shared by every text serializer (localfs TSV, duostore std-kv):
// comma-joined decimal. Bounded by kMaxParts (10000 entries ≈ 100KB worst case)
std::string join_part_sizes(const std::vector<uint64_t>& sizes);
std::vector<uint64_t> parse_part_sizes(std::string_view s);

// Single source of truth for the five first-class fields: request/response header name +
// persistence key name + member pointer. Extraction, echoing, and each backend's
// serialization all iterate this table -- adding a field only touches this spot, avoiding
// the half-done "stored but never returned" state
struct StdMetaField {
    const char* header;              // S3 request/response header name
    const char* store_key;           // key name used for backend persistence
    std::string ObjectMeta::* field;
    // false = persisted/extracted via the table but not echoed as a response header
    // (tagging answers via x-amz-tagging-count / ?tagging instead)
    bool echo = true;
};
inline constexpr StdMetaField kStdMetaFields[] = {
    {"Cache-Control", "cache_control", &ObjectMeta::cache_control},
    {"Content-Disposition", "content_disposition", &ObjectMeta::content_disposition},
    {"Content-Encoding", "content_encoding", &ObjectMeta::content_encoding},
    {"Content-Language", "content_language", &ObjectMeta::content_language},
    {"Expires", "expires", &ObjectMeta::expires},
    {"x-amz-website-redirect-location", "website_redirect", &ObjectMeta::website_redirect},
    {"x-amz-tagging", "tagging", &ObjectMeta::tagging, /*echo=*/false},
};

struct ObjectStream {
    ObjectMeta meta;                         // size is the full object length
    std::unique_ptr<http::BodyReader> body;  // already trimmed to the range
    std::optional<ByteRange> range;          // the effective range (suffix resolved / clamped)
};

struct PutResult {
    std::string etag;
    // Filled by complete_multipart when a composite checksum was computed from the
    // stored per-part values (roadmap §2.2); the handler echoes it in the response XML
    std::string checksum_algorithm;
    std::string checksum_value;
    std::string checksum_type;
};

// Conditional PUT (docs/s3-protocol.md §6): the check and the commit must both happen
// inside the backend's own atomic commit point (commit critical section / metadata CAS)
// -- a concurrent write inside an L2-level "head then put" window would break both the
// overwrite-protection and optimistic-concurrency semantics, and cross-instance it does
// not hold at all.
// Semantics: if_none_match (maps to If-None-Match: *) requires the object not to exist,
// violation throws PreconditionFailed; if_match_etag requires the current ETag to be equal,
// mismatch throws PreconditionFailed and a missing object throws NoSuchKey. The two are
// mutually exclusive (guaranteed by L2). On condition failure the backend must leave no
// trace of the write.
struct PutCondition {
    bool if_none_match = false;
    std::optional<std::string> if_match_etag;
    bool active() const { return if_none_match || if_match_etag.has_value(); }
};

struct ListOptions {
    std::string prefix;
    std::string delimiter;    // arbitrary string ("" = no grouping); grouping is a generic substring find
    int max_keys = 1000;
    std::string start_after;  // continuation-token / start-after (a key value)
};

struct ListResult {
    std::vector<ObjectMeta> objects;
    std::vector<std::string> common_prefixes;
    bool is_truncated = false;
    std::string next_token;
};

struct BucketInfo {
    std::string name;
    std::chrono::system_clock::time_point created;
};

// One entry of a CompleteMultipartUpload request: part number and ETag declared by the client
struct PartInfo {
    int part_no = 0;
    std::string etag;  // may be quoted; quotes are stripped before comparison
    // Optional client-declared part checksum from the complete XML (roadmap §2.2):
    // validated at L2 against the stored per-part value; never trusted as a source
    std::string checksum_algorithm;
    std::string checksum_value;  // base64
};

// Client-declared, gateway-verified checksum accompanying an UploadPart body
// (roadmap §2.2): persisted with the part record so complete can compute the
// composite ("-N") object checksum from verified values only
struct PartChecksum {
    std::string algorithm;  // uppercase wire name: CRC32 / CRC32C / SHA1 / SHA256
    std::string value;      // base64; empty for trailer-form uploads until the body drains
    std::shared_ptr<const std::string> pending;  // trailer capture slot (see ObjectMeta)
    std::string resolved() const {
        if (!value.empty()) return value;
        return pending ? *pending : std::string();
    }
};

// ListParts result entry
struct PartMeta {
    int part_no = 0;
    uint64_t size = 0;
    std::string etag;
    std::chrono::system_clock::time_point last_modified;
    // Verified checksum recorded at upload time; empty = the part carried none
    std::string checksum_algorithm;
    std::string checksum_value;  // base64
};

// ListMultipartUploads result entry
struct UploadInfo {
    std::string key;
    std::string upload_id;
    std::chrono::system_clock::time_point initiated;
};

// Pagination for the two multipart listing APIs (docs/archive/gaps.md §5.1). They used to return
// bare vectors with IsTruncated always false: clients took that as "reached the end", so
// with 5000 active uploads they would only ever see the first page without knowing it,
// while a single request built the whole table in memory
struct ListPartsOptions {
    int max_parts = 1000;
    int part_number_marker = 0;  // only return parts with part_no > this value (0 = from the start)
};
struct ListPartsResult {
    std::vector<PartMeta> parts;  // ascending by part_no
    bool is_truncated = false;
    int next_part_number_marker = 0;  // meaningful only when is_truncated
};

struct ListUploadsOptions {
    std::string prefix;
    std::string delimiter;  // arbitrary string ("" = no grouping), same semantics as ListOptions
    int max_uploads = 1000;
    // (key_marker, upload_id_marker) form a composite cursor: only entries strictly greater
    // than the pair are returned. An empty upload_id_marker means "key > key_marker"
    std::string key_marker;
    std::string upload_id_marker;
};
struct ListUploadsResult {
    std::vector<UploadInfo> uploads;  // ascending by (key, upload_id)
    std::vector<std::string> common_prefixes;
    bool is_truncated = false;
    std::string next_key_marker;
    std::string next_upload_id_marker;
};

struct IStorageBackend {
    // ---- bucket ----
    virtual Task<void> create_bucket(std::string_view bucket) = 0;
    virtual Task<void> delete_bucket(std::string_view bucket) = 0;  // must be empty
    virtual Task<bool> bucket_exists(std::string_view bucket) = 0;
    virtual Task<std::vector<BucketInfo>> list_buckets() = 0;

    // ---- object ----
    virtual Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                          std::optional<ByteRange> range) = 0;
    // body contract (same for put_object / upload_part): implementations must read body to
    // EOF (read returns 0), not stop once length() bytes are consumed -- the upper layer's
    // verification decorators (x-amz-content-sha256 / aws-chunked signature chain) hook in
    // at full-read and EOF, so skipping the trailing read skips the check;
    // body.read throwing => the backend must not commit the object (staging discarded /
    // remote transfer aborted);
    // when cond.active(), validate atomically at the commit point per the PutCondition contract
    virtual Task<PutResult> put_object(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta, http::BodyReader& body,
                                       PutCondition cond = {}) = 0;
    virtual Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) = 0;
    // Same-backend copy fast path (docs/archive/gaps.md §6.3): when src and dst both belong to this
    // backend, the CopyObject handler tries this hook first. Returning nullopt = no fast
    // path / unavailable this time (tier stub, cross-device, etc.); the caller falls back
    // to "get_object streaming read + put_object streaming write" -- semantically
    // equivalent, just one extra byte copy (local) or two extra network trips (cloud).
    // meta is the final object metadata (REPLACE already assembled by the handler; COPY
    // copied from the source); key/size/etag are filled in from the source by the
    // implementation -- bytes are unchanged, so etag always equals the source's
    virtual Task<std::optional<PutResult>> copy_object_fast(std::string_view /*src_bucket*/,
                                                            std::string_view /*src_key*/,
                                                            std::string_view /*dst_bucket*/,
                                                            std::string_view /*dst_key*/,
                                                            ObjectMeta /*meta*/) {
        co_return std::nullopt;
    }
    // GET ?partNumber support (roadmap §2.5): byte extent of one part of a completed
    // multipart object. The default resolves nothing — L2 falls back to the part_sizes
    // layout in ObjectMeta; proxy backends whose remote owns the layout (cloudproxy)
    // override this with a remote lookup. nullopt = layout unknown here
    struct ObjectPartExtent {
        uint64_t offset = 0;
        uint64_t size = 0;
        int parts_count = 0;
    };
    virtual Task<std::optional<ObjectPartExtent>> resolve_object_part(
        std::string_view /*bucket*/, std::string_view /*key*/, int /*part_no*/) {
        co_return std::nullopt;
    }

    // PUT/DELETE ?tagging (roadmap §2.5): replace an existing object's tag set in place
    // (canonical URL-encoded form; empty = delete all tags) without rewriting the data.
    // Default is an honest 501 — backends whose meta lives inside an atomic data commit
    // record (duostore) have no in-place meta update primitive yet; tags supplied at
    // PUT/CreateMultipartUpload time still persist everywhere via ObjectMeta.tagging
    virtual Task<void> set_object_tagging(std::string_view /*bucket*/, std::string_view /*key*/,
                                          std::string /*tagging*/) {
        throw s3::S3Error(s3::S3ErrorCode::NotImplemented,
                          "In-place object tag modification is not implemented for this "
                          "storage backend; set x-amz-tagging when writing the object.");
        co_return;  // unreachable; keeps this a coroutine
    }

    // S3 semantics: also return success for a non-existent key (idempotent delete)
    virtual Task<void> delete_object(std::string_view bucket, std::string_view key) = 0;
    virtual Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) = 0;

    // ---- multipart (docs/storage-backend.md §1/§3.2) ----
    // Returns upload_id; meta carries the desired content_type/user_meta, applied at complete
    virtual Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                               ObjectMeta meta) = 0;
    // part_no ∈ [1,10000]; re-uploading the same number is last-write-wins; returns the
    // part's ETag (content MD5). checksum (roadmap §2.2): nullopt = no checksum declared;
    // implementations persist checksum->resolved() with the part record AFTER draining
    // the body (trailer-form values only exist by then). Overrides must re-export the
    // convenience overload with `using IStorageBackend::upload_part;`
    virtual Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                        std::string_view upload_id, int part_no,
                                        http::BodyReader& body,
                                        const std::optional<PartChecksum>& checksum) = 0;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) {
        return upload_part(bucket, key, upload_id, part_no, body, std::nullopt);
    }
    // parts must have strictly increasing part numbers and ETags matching the uploaded parts;
    // total ETag = md5(concatenation of each part's binary md5)-N (same rule as S3)
    virtual Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                               std::string_view upload_id,
                                               std::span<const PartInfo> parts) = 0;
    virtual Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id) = 0;
    // Ascending by part_no, report is_truncated truthfully; missing upload throws NoSuchUpload.
    // Pagination semantics are defined once by apply_parts_page in storage/listing.h;
    // implementations may push the marker down first and then hand off to it, but must not
    // invent their own truncation rules
    virtual Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key,
                                             std::string_view upload_id,
                                             const ListPartsOptions& opt) = 0;
    // Active uploads of the bucket, ascending by (key, upload_id), report is_truncated truthfully
    virtual Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket,
                                                           const ListUploadsOptions& opt) = 0;

    virtual Task<void> close() { co_return; }
    virtual ~IStorageBackend() = default;
};

// Internal reserved bucket name (credential persistence, docs/credential-management.md §4.1).
// Only validation calls with allow_reserved=true may pass -- i.e. only CredentialStore
inline constexpr std::string_view kSysBucketName = ".sys";

// bucket/key validity checks (shared by all backends); throws S3Error when invalid.
// The bucket check is the **only** defense line for reserved names and path safety: L2's
// dispatch calls it for every request before routing (allow_reserved=false), and each
// backend's data-plane entry calls it again as defense in depth.
// User requests can never obtain allow_reserved=true
void validate_bucket_name(std::string_view bucket, bool allow_reserved = false);
// AWS general constraints: non-empty, ≤1024B, no control characters, no '.'/'..' segments
// (the latter is unsafe for any forwarding backend that splices the key into a URL path --
// RFC 3986 dot-segment normalization would rewrite the object's identity)
void validate_object_key(std::string_view key);
// Additional constraints for path-mapping backends (localfs/xlocalfs) (docs/archive/gaps.md §6.3):
// no leading '/', no empty segments, each segment ≤255B. Trailing-'/' directory-marker
// objects are **not** forbidden -- localfs represents them with a reserved marker file
// inside the directory; the S3 console's "create folder" and the directory semantics of
// s3fs/goofys/rclone depend on this form. Callers must call validate_object_key first
void validate_fs_object_key(std::string_view key);

// Argument for backends' internal validation: backends must be able to serve
// CredentialStore's reads/writes of .sys, so this layer admits the reserved name. What it
// validates is **path safety** (character set, length, no '/' or NUL); blocking the
// reserved name is the job of L2 dispatch (which always uses the default
// allow_reserved=false)
inline constexpr bool kAllowReserved = true;

}  // namespace lights3::storage
