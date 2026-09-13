// Iceberg metadata pure functions (docs/architecture/s3-tables-design.md §7, §13)
#include <nlohmann/json.hpp>

#include "tables/iceberg/metadata.h"
#include "tables/iceberg/requirements.h"
#include "tables/iceberg/transition.h"
#include "tables/iceberg/updates.h"
#include "tables/identifier.h"
#include "tables/rest_error.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::tables;
using namespace lights3::tables::iceberg;
using nlohmann::json;

namespace {

json simple_schema() {
    return json::parse(R"({"type":"struct","fields":[
        {"id":7,"name":"id","required":true,"type":"long"},
        {"id":9,"name":"tags","required":false,"type":{"type":"list","element-id":11,"element":"string","element-required":false}},
        {"id":10,"name":"attrs","required":false,"type":{"type":"map","key-id":12,"key":"string","value-id":13,"value":{"type":"struct","fields":[{"id":14,"name":"x","required":false,"type":"int"}]},"value-required":false}}
    ]})");
}

CreateTableInput input(int fv = 2) {
    CreateTableInput in;
    in.name = "t";
    in.schema = simple_schema();
    in.partition_spec = json::parse(R"({"fields":[{"source-id":7,"name":"id_bucket","transform":"bucket[8]"}]})");
    in.write_order = json::parse(
        R"({"fields":[{"source-id":7,"transform":"identity","direction":"asc","null-order":"nulls-first"}]})");
    in.properties = {{"k", "v"}};
    in.format_version = fv;
    in.location = "s3://b/ns/t";
    in.table_uuid = "0e7c4f3a-0000-4000-8000-000000000001";
    in.now_ms = 1000;
    return in;
}

ApplyOptions opts() { return ApplyOptions{"b", ".lights3-table/"}; }

json snapshot(int64_t id, int64_t seq, const char* op = "append", std::optional<int64_t> parent = std::nullopt) {
    json s;
    s["snapshot-id"] = id;
    s["sequence-number"] = seq;
    s["timestamp-ms"] = 1000 + id;
    s["manifest-list"] = "s3://b/ns/t/metadata/snap-" + std::to_string(id) + ".avro";
    s["summary"] = json::object({{"operation", op}});
    if (parent) s["parent-snapshot-id"] = *parent;
    return s;
}

template <class F>
int status_of(F&& f) {
    try {
        f();
    } catch (const RestError& e) {
        return e.status;
    }
    return 0;
}

}  // namespace

TEST(tables_iceberg_initial_metadata_assigns_fresh_ids) {
    json md = initial_metadata(input());
    CHECK_EQ(md["format-version"].get<int>(), 2);
    CHECK_EQ(md["current-schema-id"].get<int>(), 0);
    // pre-order: id=1, tags=2, tags.element=3, attrs=4, key=5, value=6, value.x=7
    auto ids = schema_field_ids(md["schemas"][0]);
    CHECK_EQ(ids.size(), size_t(7));
    for (size_t i = 0; i < ids.size(); ++i) CHECK_EQ(ids[i], int64_t(i + 1));
    CHECK_EQ(md["last-column-id"].get<int>(), 7);
    // partition source remapped 7 -> 1, field id from 1000
    CHECK_EQ(md["partition-specs"][0]["fields"][0]["source-id"].get<int>(), 1);
    CHECK_EQ(md["partition-specs"][0]["fields"][0]["field-id"].get<int>(), 1000);
    CHECK_EQ(md["last-partition-id"].get<int>(), 1000);
    CHECK_EQ(md["sort-orders"][0]["order-id"].get<int>(), 1);
    CHECK_EQ(md["default-sort-order-id"].get<int>(), 1);
    CHECK_EQ(md["last-sequence-number"].get<int>(), 0);
    CHECK_EQ(md["current-snapshot-id"].get<int>(), -1);
    CHECK(!md.contains("schema"));
    CHECK_EQ(md["properties"]["k"].get<std::string>(), "v");
    // canonical is stable
    CHECK_EQ(canonical(md), canonical(json::parse(canonical(md))));
}

TEST(tables_iceberg_initial_metadata_v1_mirrors_and_empty_order) {
    auto in = input(1);
    in.write_order.reset();
    json md = initial_metadata(in);
    CHECK_EQ(md["format-version"].get<int>(), 1);
    CHECK(md.contains("schema"));
    CHECK(md.contains("partition-spec"));
    CHECK(!md.contains("last-sequence-number"));
    CHECK_EQ(md["sort-orders"][0]["order-id"].get<int>(), 0);
    CHECK_EQ(status_of([] {
                 auto i = input(3);
                 initial_metadata(i);
             }),
             406);
    CHECK_EQ(status_of([] {
                 auto i = input();
                 i.partition_spec = json::parse(R"({"fields":[{"source-id":7,"name":"x","transform":"nope"}]})");
                 initial_metadata(i);
             }),
             400);
    CHECK_EQ(status_of([] {
                 auto i = input();
                 i.partition_spec = json::parse(R"({"fields":[{"source-id":99,"name":"x","transform":"identity"}]})");
                 initial_metadata(i);
             }),
             400);
}

TEST(tables_iceberg_parse_and_validate) {
    json md = initial_metadata(input());
    json ok = parse_and_validate(canonical(md), 1 << 20);
    CHECK_EQ(ok["table-uuid"].get<std::string>(), md["table-uuid"].get<std::string>());
    CHECK_EQ(status_of([] { parse_and_validate("{not json", 1 << 20); }), 400);
    CHECK_EQ(status_of([&] { parse_and_validate(canonical(md), 10); }), 400);
    json v3 = md;
    v3["format-version"] = 3;
    CHECK_EQ(status_of([&] { validate_metadata(v3); }), 406);
    json bad = md;
    bad["current-schema-id"] = 42;
    CHECK_EQ(status_of([&] { validate_metadata(bad); }), 400);
    json dup = md;
    dup["schemas"][0]["fields"][1]["id"] = 1;
    CHECK_EQ(status_of([&] { validate_metadata(dup); }), 400);
    // v1 mirrors are turned into lists
    json v1 = json::parse(R"({"format-version":1,"table-uuid":"u","location":"s3://b/x","last-column-id":1,
        "schema":{"type":"struct","fields":[{"id":1,"name":"a","required":true,"type":"int"}]},
        "partition-spec":[]})");
    validate_metadata(v1);
    CHECK_EQ(v1["schemas"].size(), size_t(1));
    CHECK_EQ(v1["partition-specs"][0]["spec-id"].get<int>(), 0);
    CHECK_EQ(v1["sort-orders"][0]["order-id"].get<int>(), 0);
}

TEST(tables_iceberg_requirements) {
    json md = initial_metadata(input());
    json with_ref = apply_updates(
        md,
        json::array(
            {json::parse(R"({"action":"add-snapshot","snapshot":)" + snapshot(1, 1).dump() + "}"),
             json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":1})")}),
        opts());
    auto req = [](const std::string& s) { return json::array({json::parse(s)}); };
    // pass
    check_requirements(md, req(R"({"type":"assert-table-uuid","uuid":"0e7c4f3a-0000-4000-8000-000000000001"})"), true);
    check_requirements(md, req(R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null})"), true);
    check_requirements(with_ref, req(R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":1})"), true);
    check_requirements(md, req(R"({"type":"assert-last-assigned-field-id","last-assigned-field-id":7})"), true);
    check_requirements(md, req(R"({"type":"assert-current-schema-id","current-schema-id":0})"), true);
    check_requirements(md, req(R"({"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1000})"),
                       true);
    check_requirements(md, req(R"({"type":"assert-default-spec-id","default-spec-id":0})"), true);
    check_requirements(md, req(R"({"type":"assert-default-sort-order-id","default-sort-order-id":1})"), true);
    check_requirements(md, req(R"({"type":"assert-create"})"), false);
    // fail -> 409
    auto fails = [&](const json& m, const std::string& s) {
        return status_of([&] { check_requirements(m, req(s), true); });
    };
    CHECK_EQ(fails(md, R"({"type":"assert-create"})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-table-uuid","uuid":"other"})"), 409);
    CHECK_EQ(fails(with_ref, R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":5})"), 409);
    CHECK_EQ(fails(with_ref, R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":5})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-last-assigned-field-id","last-assigned-field-id":1})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-current-schema-id","current-schema-id":3})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-last-assigned-partition-id","last-assigned-partition-id":5})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-default-spec-id","default-spec-id":2})"), 409);
    CHECK_EQ(fails(md, R"({"type":"assert-default-sort-order-id","default-sort-order-id":0})"), 409);
    // unknown / malformed -> 400
    CHECK_EQ(fails(md, R"({"type":"assert-something"})"), 400);
    CHECK_EQ(fails(md, R"({"type":"assert-ref-snapshot-id"})"), 400);
    CHECK_EQ(status_of([&] { check_requirements(md, json::object(), true); }), 400);
}

TEST(tables_iceberg_updates_snapshots_and_refs) {
    json md = initial_metadata(input());
    json u = json::array();
    u.push_back(json::parse(R"({"action":"add-snapshot","snapshot":)" + snapshot(1, 1).dump() + "}"));
    u.push_back(json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":1})"));
    json n1 = apply_updates(md, u, opts());
    CHECK_EQ(current_snapshot_id(n1), 1);
    CHECK_EQ(n1["last-sequence-number"].get<int>(), 1);
    CHECK_EQ(n1["snapshot-log"].size(), size_t(1));
    check_transition(md, n1);
    // second snapshot with parent, tag ref, then remove the first
    json u2 = json::array();
    u2.push_back(json::parse(R"({"action":"add-snapshot","snapshot":)" + snapshot(2, 2, "overwrite", 1).dump() + "}"));
    u2.push_back(json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":2})"));
    u2.push_back(json::parse(
        R"({"action":"set-snapshot-ref","ref-name":"v1","type":"tag","snapshot-id":1,"max-ref-age-ms":100})"));
    json n2 = apply_updates(n1, u2, opts());
    CHECK_EQ(current_snapshot_id(n2), 2);
    CHECK_EQ(n2["refs"]["v1"]["max-ref-age-ms"].get<int>(), 100);
    check_transition(n1, n2);
    json n3 = apply_updates(n2, json::array({json::parse(R"({"action":"remove-snapshots","snapshot-ids":[1]})")}),
                            opts());
    CHECK_EQ(n3["snapshots"].size(), size_t(1));
    CHECK(!n3["refs"].contains("v1"));
    CHECK_EQ(current_snapshot_id(n3), 2);
    json n4 = apply_updates(n3, json::array({json::parse(R"({"action":"remove-snapshot-ref","ref-name":"main"})")}),
                            opts());
    CHECK_EQ(current_snapshot_id(n4), -1);
    // errors
    auto err = [&](const json& base, const std::string& s) {
        return status_of([&] { apply_updates(base, json::array({json::parse(s)}), opts()); });
    };
    CHECK_EQ(err(n1, R"({"action":"add-snapshot","snapshot":)" + snapshot(1, 5).dump() + "}"), 409);
    CHECK_EQ(err(n1, R"({"action":"add-snapshot","snapshot":)" + snapshot(3, 1).dump() + "}"), 409);
    CHECK_EQ(err(n1, R"({"action":"add-snapshot","snapshot":)" + snapshot(3, 3, "append", 77).dump() + "}"), 409);
    CHECK_EQ(
        err(md,
            R"({"action":"add-snapshot","snapshot":{"snapshot-id":5,"timestamp-ms":1,"manifest-list":"s3://b/x"}})"),
        400);
    CHECK_EQ(
        err(md,
            R"({"action":"add-snapshot","snapshot":{"snapshot-id":5,"sequence-number":1,"timestamp-ms":1,"manifests":["s3://b/x"]}})"),
        400);
    CHECK_EQ(
        err(md,
            R"({"action":"add-snapshot","snapshot":{"snapshot-id":5,"sequence-number":1,"timestamp-ms":1,"manifest-list":"s3://other/x"}})"),
        409);
    CHECK_EQ(
        err(md,
            R"({"action":"add-snapshot","snapshot":{"snapshot-id":5,"sequence-number":1,"timestamp-ms":1,"manifest-list":"s3://b/x","summary":{"operation":"bogus"}}})"),
        400);
    CHECK_EQ(err(md, R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":99})"), 409);
    CHECK_EQ(err(md, R"({"action":"set-snapshot-ref","ref-name":"main","type":"weird","snapshot-id":1})"), 400);
    CHECK_EQ(err(md, R"({"action":"nonsense"})"), 400);
    CHECK_EQ(err(md, R"({"action":"add-encryption-key","encryption-key":{}})"), 406);
    CHECK_EQ(status_of([&] { apply_updates(md, json::object(), opts()); }), 400);
}

TEST(tables_iceberg_updates_schema_spec_order_props) {
    json md = initial_metadata(input());
    // add a column (id 8 > last-column-id 7), promote int -> long on x (id 7)
    json schema = md["schemas"][0];
    schema["fields"].push_back(json::parse(R"({"id":8,"name":"added","required":false,"type":"string"})"));
    schema["fields"][2]["type"]["value"]["fields"][0]["type"] = "long";
    json u = json::array();
    u.push_back(json::object({{"action", "add-schema"}, {"schema", schema}, {"last-column-id", 8}}));
    u.push_back(json::parse(R"({"action":"set-current-schema","schema-id":-1})"));
    json n1 = apply_updates(md, u, opts());
    CHECK_EQ(n1["current-schema-id"].get<int>(), 1);
    CHECK_EQ(n1["last-column-id"].get<int>(), 8);
    check_transition(md, n1);
    // identical schema is reused, not duplicated
    json n1b = apply_updates(n1,
                             json::array({json::object({{"action", "add-schema"}, {"schema", schema}}),
                                          json::parse(R"({"action":"set-current-schema","schema-id":-1})")}),
                             opts());
    CHECK_EQ(n1b["schemas"].size(), size_t(2));
    // illegal: long -> int, optional -> required, reused retired id
    json bad = schema;
    bad["fields"][0]["type"] = "int";
    CHECK_EQ(status_of([&] {
                 apply_updates(n1, json::array({json::object({{"action", "add-schema"}, {"schema", bad}})}), opts());
             }),
             400);
    json bad2 = schema;
    bad2["fields"][3]["required"] = true;
    CHECK_EQ(status_of([&] {
                 apply_updates(n1, json::array({json::object({{"action", "add-schema"}, {"schema", bad2}})}), opts());
             }),
             400);
    json bad3 = schema;
    bad3["fields"].erase(3);
    bad3["fields"].push_back(json::parse(R"({"id":8,"name":"reused","required":false,"type":"int"})"));
    // same id 8 with another type: not a promotion of "added" (string) -> rejected
    CHECK_EQ(status_of([&] {
                 apply_updates(n1, json::array({json::object({{"action", "add-schema"}, {"schema", bad3}})}), opts());
             }),
             400);
    // spec: server assigns field ids and spec id; source must exist
    json n2 = apply_updates(
        n1,
        json::array(
            {json::parse(
                 R"({"action":"add-spec","spec":{"fields":[{"source-id":8,"name":"added_trunc","transform":"truncate[4]"}]}})"),
             json::parse(R"({"action":"set-default-spec","spec-id":-1})")}),
        opts());
    CHECK_EQ(n2["default-spec-id"].get<int>(), 1);
    CHECK_EQ(n2["partition-specs"][1]["fields"][0]["field-id"].get<int>(), 1001);
    CHECK_EQ(n2["last-partition-id"].get<int>(), 1001);
    CHECK_EQ(
        status_of([&] {
            apply_updates(
                n1,
                json::array({json::parse(
                    R"({"action":"add-spec","spec":{"fields":[{"source-id":99,"name":"x","transform":"identity"}]}})")}),
                opts());
        }),
        400);
    CHECK_EQ(status_of([&] {
                 apply_updates(n1, json::array({json::parse(R"({"action":"set-default-spec","spec-id":9})")}), opts());
             }),
             400);
    CHECK_EQ(status_of([&] {
                 apply_updates(n2, json::array({json::parse(R"({"action":"remove-partition-specs","spec-ids":[1]})")}),
                               opts());
             }),
             400);
    json n2b = apply_updates(n2, json::array({json::parse(R"({"action":"remove-partition-specs","spec-ids":[0]})")}),
                             opts());
    CHECK_EQ(n2b["partition-specs"].size(), size_t(1));
    // sort order
    json n3 = apply_updates(n2,
                            json::array({json::parse(R"({"action":"add-sort-order","sort-order":{"fields":[]}})"),
                                         json::parse(R"({"action":"set-default-sort-order","sort-order-id":-1})")}),
                            opts());
    CHECK_EQ(n3["default-sort-order-id"].get<int>(), 0);
    json n3b = apply_updates(
        n2,
        json::array(
            {json::parse(
                 R"({"action":"add-sort-order","sort-order":{"fields":[{"source-id":8,"transform":"identity","direction":"desc","null-order":"nulls-last"}]}})"),
             json::parse(R"({"action":"set-default-sort-order","sort-order-id":-1})")}),
        opts());
    CHECK_EQ(n3b["default-sort-order-id"].get<int>(), 2);
    // properties, location, uuid, format version
    json n4 = apply_updates(
        n3,
        json::array({json::parse(R"({"action":"set-properties","updates":{"a":"1","k":"v2"}})"),
                     json::parse(R"({"action":"remove-properties","removals":["k"]})"),
                     json::parse(R"({"action":"set-location","location":"s3://b/ns/elsewhere/"})")}),
        opts());
    CHECK_EQ(n4["properties"]["a"].get<std::string>(), "1");
    CHECK(!n4["properties"].contains("k"));
    CHECK_EQ(n4["location"].get<std::string>(), "s3://b/ns/elsewhere");
    CHECK_EQ(status_of([&] {
                 apply_updates(n3, json::array({json::parse(R"({"action":"set-properties","updates":{"a":1}})")}),
                               opts());
             }),
             400);
    CHECK_EQ(status_of([&] {
                 apply_updates(
                     n3, json::array({json::parse(R"({"action":"set-properties","updates":{"format-version":"2"}})")}),
                     opts());
             }),
             400);
    CHECK_EQ(status_of([&] {
                 apply_updates(
                     n3,
                     json::array({json::parse(R"({"action":"set-location","location":"s3://b/.lights3-table/x"})")}),
                     opts());
             }),
             400);
    CHECK_EQ(status_of([&] {
                 apply_updates(n3, json::array({json::parse(R"({"action":"set-location","location":"s3://other/x"})")}),
                               opts());
             }),
             400);
    CHECK_EQ(status_of([&] {
                 apply_updates(n3, json::array({json::parse(R"({"action":"assign-uuid","uuid":"zzz"})")}), opts());
             }),
             409);
    apply_updates(
        n3, json::array({json::parse(R"({"action":"assign-uuid","uuid":"0e7c4f3a-0000-4000-8000-000000000001"})")}),
        opts());
    CHECK_EQ(status_of([&] {
                 apply_updates(n3,
                               json::array({json::parse(R"({"action":"upgrade-format-version","format-version":3})")}),
                               opts());
             }),
             406);
    CHECK_EQ(status_of([&] {
                 apply_updates(n3,
                               json::array({json::parse(R"({"action":"upgrade-format-version","format-version":1})")}),
                               opts());
             }),
             400);
    // v1 -> v2 upgrade adds sequence numbers
    json v1 = initial_metadata(input(1));
    json v2 = apply_updates(v1, json::array({json::parse(R"({"action":"upgrade-format-version","format-version":2})")}),
                            opts());
    CHECK_EQ(v2["format-version"].get<int>(), 2);
    CHECK_EQ(v2["last-sequence-number"].get<int>(), 0);
    CHECK(!v2.contains("schema"));
    check_transition(v1, v2);
}

TEST(tables_iceberg_transition_invariants) {
    json md = initial_metadata(input());
    auto violates = [&](const json& next) { return status_of([&] { check_transition(md, next); }); };
    json a = md;
    a["table-uuid"] = "other";
    CHECK_EQ(violates(a), 409);
    json b = md;
    b["schemas"][0]["fields"][0]["name"] = "renamed-in-place";
    CHECK_EQ(violates(b), 409);
    json c = md;
    c["last-column-id"] = 3;
    CHECK_EQ(violates(c), 409);
    json d = md;
    d["format-version"] = 1;
    CHECK_EQ(violates(d), 409);
    json e = md;
    e["schemas"].push_back(json::parse(
        R"({"schema-id":1,"type":"struct","fields":[{"id":3,"name":"reuse","required":false,"type":"int"}]})"));
    CHECK_EQ(violates(e), 409);
    // removing a schema is allowed, the remaining ones must be byte-identical
    json f = md;
    f["schemas"].push_back(
        json::parse(R"({"schema-id":1,"type":"struct","fields":[{"id":9,"name":"n","required":false,"type":"int"}]})"));
    f["last-column-id"] = 9;
    CHECK_EQ(violates(f), 0);
}

TEST(tables_iceberg_finish_transition) {
    json md = initial_metadata(input());
    md["snapshot-log"] = json::array({json::parse(R"({"snapshot-id":77,"timestamp-ms":1})")});
    json n = md;
    finish_transition(n, "s3://b/.lights3-table/ns/t/metadata/00001-x.metadata.json", 5000, MetadataLimits{2});
    CHECK_EQ(n["snapshot-log"].size(), size_t(0));
    CHECK_EQ(n["metadata-log"].size(), size_t(1));
    CHECK_EQ(n["metadata-log"][0]["metadata-file"].get<std::string>(),
             "s3://b/.lights3-table/ns/t/metadata/00001-x.metadata.json");
    CHECK_EQ(n["metadata-log"][0]["timestamp-ms"].get<int>(), 1000);
    CHECK_EQ(n["last-updated-ms"].get<int>(), 5000);
    finish_transition(n, "s3://b/two", 6000, MetadataLimits{2});
    finish_transition(n, "s3://b/three", 7000, MetadataLimits{2});
    CHECK_EQ(n["metadata-log"].size(), size_t(2));
    CHECK_EQ(n["metadata-log"][0]["metadata-file"].get<std::string>(), "s3://b/two");
}

TEST(tables_identifier_rules_and_paths) {
    CHECK(valid_segment("a"));
    CHECK(valid_segment("sales_2024-q1"));
    CHECK(!valid_segment(""));
    CHECK(!valid_segment("Sales"));
    CHECK(!valid_segment("-x"));
    CHECK(!valid_segment("x_"));
    CHECK(!valid_segment("a.b"));
    CHECK(!valid_segment("tbl"));
    CHECK(!valid_segment(std::string(65, 'a')));
    CHECK(valid_segment(std::string(64, 'a')));
    auto l = parse_namespace_path("sales%1Feu");
    CHECK_EQ(l.size(), size_t(2));
    CHECK_EQ(ns_path(l), "sales/eu");
    CHECK_EQ(ns_display(l), "sales.eu");
    CHECK_EQ(parse_namespace_path("a.b.c").size(), size_t(3));
    CHECK_EQ(parse_namespace_json(json::parse(R"(["x","y"])")).size(), size_t(2));
    CHECK_EQ(status_of([] { parse_namespace_path(""); }), 400);
    CHECK_EQ(status_of([] { parse_namespace_path("bad%2Fname"); }), 400);
    CHECK_EQ(status_of([] { parse_namespace_json(json::parse(R"([1])")); }), 400);
    CHECK_EQ(location_to_key("b", ".lights3-table/", "s3://b/ns/t/"), "ns/t");
    CHECK_EQ(location_to_key("b", ".lights3-table/", "s3a://b/ns/t"), "ns/t");
    CHECK_EQ(status_of([] { location_to_key("b", ".lights3-table/", "s3://c/ns/t"); }), 400);
    CHECK_EQ(status_of([] { location_to_key("b", ".lights3-table/", "s3://b/.lights3-table/x"); }), 400);
    CHECK_EQ(status_of([] { location_to_key("b", ".lights3-table/", "s3://b/a/../x"); }), 400);
    CHECK_EQ(status_of([] { location_to_key("b", ".lights3-table/", "s3://b/"); }), 400);
    CHECK_EQ(path_to_key("b", "s3://b/x/y.avro"), "x/y.avro");
    CHECK_EQ(status_of([] { path_to_key("b", "file:///x"); }), 409);
    CHECK_EQ(base64url_decode(base64url("hello?/+=")), "hello?/+=");
    CHECK(looks_like_uuid(new_uuid()));
    CHECK(!looks_like_uuid("nope"));
    CHECK_EQ(random_hex16().size(), size_t(32));
}
