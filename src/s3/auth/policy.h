// per-credential policy（docs/credential-management.md §10.4）。
// Standalone header: the signature layer (sigv4.h) must carry a policy snapshot at verify time (docs/gaps.md §3.7),
// while credential_store.h depends on sigv4.h -- moving this back would create a cycle.
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::s3 {

// Action granularity (docs/gaps.md §5.10): previously the only switch was readonly, so "can write" implied
// "can delete" -- making it impossible to express the most common backup policy (write-only, no delete).
// Classification is by consequence, not HTTP method: DeleteObjects is a POST yet clearly belongs to Delete
enum class Action { Read, Write, Delete };

const char* action_name(Action a);
// Parse an action name from JSON; unknown names return nullopt (caller converts to InvalidRequest)
std::optional<Action> action_from_name(std::string_view s);

// Default (nullopt) = unrestricted
struct CredentialPolicy {
    std::vector<std::string> buckets;   // bucket glob allowlist; empty = all
    std::vector<std::string> prefixes;  // key prefix allowlist; empty = all (§5.10)
    bool readonly = false;              // equivalent to actions = [read], kept for compatibility
    std::vector<Action> actions;        // empty = determined by readonly

    // Empty bucket means an account-level operation (ListBuckets). Empty key means "this check is
    // unrelated to a specific object" (create bucket, list objects in a bucket, etc.); prefixes are not checked then
    bool allows(std::string_view bucket, std::string_view key, Action action) const;
    bool allows_action(Action a) const;
    bool allows_bucket(std::string_view bucket) const;
    // Prefix filtering for listing results (with multi-tenant shared buckets, prefixes are the isolation boundary):
    // allows_key = the key falls under some allowlisted prefix; prefix_may_contain = the CommonPrefixes group
    // returned by a listing **may** contain allowlisted keys (either direction of the prefix relation holds)
    bool allows_key(std::string_view key) const;
    bool prefix_may_contain(std::string_view group_prefix) const;
};

// JSON field conventions for policy
// （{"buckets": [...], "prefixes": [...], "readonly": bool, "actions": [...]}），
// Shared by the admin handler, on-disk objects, and credentials_file; implemented in credential_store.cc
// (nlohmann stays out of headers). Invalid/unknown fields throw S3Error(InvalidRequest)
CredentialPolicy parse_policy_json(const std::string& text);
std::string policy_to_json(const CredentialPolicy& p);

}  // namespace lights3::s3
