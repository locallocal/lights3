// Avro OCF reader + manifest model (docs/s3-tables-design.md §7.4, §13):
// hand-built minimal containers for every Avro type, the PyIceberg-written fixtures under
// tests/fixtures/tables (null and deflate codecs) checked field by field against what
// PyIceberg reads back, and the failure modes (truncation, sync mismatch, depth,
// unsupported codec)
#include <functional>
#include <nlohmann/json.hpp>

#include "tables/iceberg/avro_reader.h"
#include "tables/iceberg/manifest.h"
#include "tables/rest_error.h"
#include "unit/mini_test.h"
#include "unit/tables_fixtures.h"

using namespace lights3;
using namespace lights3::tables;
using namespace lights3::tables::iceberg;
using nlohmann::json;

namespace {

using tables_fixtures::ocf;
using tables_fixtures::put_bytes;
using tables_fixtures::put_long;
using tables_fixtures::slurp;

std::vector<json> read_all(std::string_view bytes, size_t max = 1000) {
    avro::Reader r(bytes);
    std::vector<json> out;
    r.for_each([&](json&& j) { out.push_back(std::move(j)); }, max);
    return out;
}

int status_of(const std::function<void()>& f) {
    try {
        f();
    } catch (const RestError& e) {
        return e.status;
    }
    return 0;
}

}  // namespace

TEST(tables_avro_every_type) {
    // a record with one field of each kind, nested array / map / union / enum / fixed
    std::string schema = R"({"type":"record","name":"all","fields":[
        {"name":"n","type":"null"},{"name":"b","type":"boolean"},{"name":"i","type":"int"},
        {"name":"l","type":"long"},{"name":"f","type":"float"},{"name":"d","type":"double"},
        {"name":"by","type":"bytes"},{"name":"s","type":"string"},
        {"name":"e","type":{"type":"enum","name":"col","symbols":["RED","GREEN"]}},
        {"name":"fx","type":{"type":"fixed","name":"f4","size":4}},
        {"name":"arr","type":{"type":"array","items":"long"}},
        {"name":"m","type":{"type":"map","values":"int"}},
        {"name":"u","type":["null","string"]},
        {"name":"inner","type":{"type":"record","name":"in","fields":[{"name":"x","type":"int"}]}},
        {"name":"again","type":"in"}
    ]})";
    std::string rec;
    // n
    // b = true
    rec.push_back(1);
    // i
    put_long(rec, -3);
    // l
    put_long(rec, 1LL << 40);
    // f
    float fl = 1.5f;
    rec.append(reinterpret_cast<const char*>(&fl), 4);
    // d
    double db = -2.25;
    rec.append(reinterpret_cast<const char*>(&db), 8);
    // by
    put_bytes(rec, std::string("\x00\xff", 2));
    // s
    put_bytes(rec, "héllo");
    // e = GREEN
    put_long(rec, 1);
    // fx
    rec.append("ABCD");
    // arr: a negative block count carries the byte size (Spark style), then a plain block
    std::string items;
    put_long(items, 7);
    put_long(items, 8);
    put_long(rec, -2);
    put_long(rec, static_cast<int64_t>(items.size()));
    rec += items;
    put_long(rec, 1);
    put_long(rec, 9);
    put_long(rec, 0);
    // m
    put_long(rec, 1);
    put_bytes(rec, "k");
    put_long(rec, 42);
    put_long(rec, 0);
    // u = string branch
    put_long(rec, 1);
    put_bytes(rec, "opt");
    // inner, again
    put_long(rec, 5);
    put_long(rec, 6);
    auto rows = read_all(ocf(schema, "null", {rec, rec}));
    CHECK_EQ(rows.size(), size_t(2));
    const json& j = rows[0];
    CHECK(j["n"].is_null());
    CHECK_EQ(j["b"].get<bool>(), true);
    CHECK_EQ(j["i"].get<int>(), -3);
    CHECK_EQ(j["l"].get<int64_t>(), int64_t(1LL << 40));
    CHECK_EQ(j["f"].get<float>(), 1.5f);
    CHECK_EQ(j["d"].get<double>(), -2.25);
    CHECK(j["by"].is_binary());
    CHECK_EQ(j["by"].get_binary().size(), size_t(2));
    CHECK_EQ(j["s"].get<std::string>(), "héllo");
    CHECK_EQ(j["e"].get<std::string>(), "GREEN");
    CHECK_EQ(j["fx"].get_binary().size(), size_t(4));
    CHECK_EQ(j["arr"].size(), size_t(3));
    CHECK_EQ(j["arr"][2].get<int>(), 9);
    CHECK_EQ(j["m"]["k"].get<int>(), 42);
    CHECK_EQ(j["u"].get<std::string>(), "opt");
    CHECK_EQ(j["inner"]["x"].get<int>(), 5);
    CHECK_EQ(j["again"]["x"].get<int>(), 6);
    avro::Reader r(ocf(schema, "null", {rec}));
    CHECK_EQ(std::string(r.codec()), "null");
    CHECK_EQ(r.meta().count("avro.schema"), size_t(1));
    CHECK_EQ(r.schema_json()["name"].get<std::string>(), "all");
}

TEST(tables_avro_failure_modes) {
    std::string schema = R"({"type":"record","name":"r","fields":[{"name":"s","type":"string"}]})";
    std::string rec;
    put_bytes(rec, "abc");
    std::string good = ocf(schema, "null", {rec});
    CHECK_EQ(read_all(good).size(), size_t(1));
    // truncated anywhere in the file → 409
    for (size_t cut : {size_t(3), good.size() - 20, good.size() - 1})
        CHECK_EQ(status_of([&] { read_all(good.substr(0, cut)); }), 409);
    // sync marker mismatch
    CHECK_EQ(status_of([&] { read_all(ocf(schema, "null", {rec}, true)); }), 409);
    // bad magic / unparseable schema / unknown type
    std::string bad_magic = good;
    bad_magic[0] = 'X';
    CHECK_EQ(status_of([&] { read_all(bad_magic); }), 409);
    CHECK_EQ(status_of([&] { read_all(ocf("{not json", "null", {})); }), 409);
    CHECK_EQ(status_of([&] {
                 read_all(ocf(R"({"type":"record","name":"r","fields":[{"name":"s","type":"nope"}]})", "null", {}));
             }),
             409);
    // more records than allowed
    CHECK_EQ(status_of([&] { read_all(ocf(schema, "null", {rec, rec}), 1); }), 409);
    // a record whose payload nests past the depth limit: recursive union of itself
    std::string deep_schema = R"({"type":"record","name":"n","fields":[{"name":"x","type":["null","n"]}]})";
    std::string deep;
    for (int i = 0; i < 70; ++i) put_long(deep, 1);
    put_long(deep, 0);
    CHECK_EQ(status_of([&] { read_all(ocf(deep_schema, "null", {deep})); }), 409);
    std::string shallow;
    for (int i = 0; i < 10; ++i) put_long(shallow, 1);
    put_long(shallow, 0);
    CHECK_EQ(read_all(ocf(deep_schema, "null", {shallow})).size(), size_t(1));
    // unsupported codec: the header parses, for_each refuses
    avro::Reader snappy(ocf(schema, "snappy", {rec}));
    CHECK_EQ(std::string(snappy.codec()), "snappy");
    bool unsupported = false;
    try {
        snappy.for_each([](json&&) {}, 10);
    } catch (const avro::UnsupportedCodec&) {
        unsupported = true;
    }
    CHECK(unsupported);
    // a deflate block that is not deflate data
    if (avro::deflate_supported()) CHECK_EQ(status_of([&] { read_all(ocf(schema, "deflate", {rec})); }), 409);
}

namespace {

// The C++ parse of a fixture pair against PyIceberg's own read of the same files
void check_fixture(const std::string& suffix, bool expect_deflate) {
    json expected = json::parse(slurp(suffix + ".json"));
    std::string ml = slurp("ml-" + suffix + ".avro");
    std::string m = slurp("m-" + suffix + ".avro");
    avro::Reader lr(ml);
    CHECK_EQ(std::string(lr.codec()), expect_deflate ? "deflate" : "null");
    CHECK_EQ(lr.meta().at("format-version"), "2");
    if (expect_deflate && !avro::deflate_supported()) {
        bool unsupported = false;
        try {
            parse_manifest_list(lr, 100);
        } catch (const avro::UnsupportedCodec&) {
            unsupported = true;
        }
        CHECK(unsupported);
        return;
    }
    auto manifests = parse_manifest_list(lr, 100);
    CHECK_EQ(manifests.size(), expected["manifest-list"].size());
    for (size_t i = 0; i < manifests.size(); ++i) {
        const json& e = expected["manifest-list"][i];
        CHECK_EQ(manifests[i].path, e["manifest_path"].get<std::string>());
        CHECK_EQ(manifests[i].length, e["manifest_length"].get<int64_t>());
        CHECK_EQ(manifests[i].spec_id, e["partition_spec_id"].get<int>());
        CHECK_EQ(manifests[i].content, e["content"].get<int>());
        CHECK_EQ(manifests[i].sequence_number, e["sequence_number"].get<int64_t>());
        CHECK_EQ(manifests[i].min_sequence_number, e["min_sequence_number"].get<int64_t>());
        CHECK_EQ(manifests[i].added_snapshot_id, e["added_snapshot_id"].get<int64_t>());
        // the manifest object is exactly the recorded length
        CHECK_EQ(static_cast<int64_t>(m.size()), manifests[i].length);
    }
    avro::Reader mr(m);
    CHECK_EQ(mr.meta().at("content"), "data");
    auto files = parse_manifest(mr, 1000, manifests[0].sequence_number);
    CHECK_EQ(files.size(), expected["manifest"].size());
    for (size_t i = 0; i < files.size(); ++i) {
        const json& e = expected["manifest"][i];
        CHECK_EQ(files[i].status, e["status"].get<int>());
        CHECK_EQ(files[i].path, e["file_path"].get<std::string>());
        CHECK_EQ(files[i].format, e["file_format"].get<std::string>());
        CHECK_EQ(files[i].content, e["content"].get<int>());
        CHECK_EQ(files[i].size_bytes, e["file_size_in_bytes"].get<int64_t>());
        CHECK_EQ(files[i].record_count, e["record_count"].get<int64_t>());
        CHECK(files[i].snapshot_id.has_value());
        CHECK_EQ(*files[i].snapshot_id, e["snapshot_id"].get<int64_t>());
        CHECK(files[i].sequence_number.has_value());
        CHECK_EQ(*files[i].sequence_number, e["sequence_number"].get<int64_t>());
    }
}

}  // namespace

TEST(tables_avro_pyiceberg_fixtures) {
    check_fixture("1", false);
    check_fixture("1-deflate", true);
    check_fixture("2-readd", false);
    check_fixture("3-delete", false);
    check_fixture("4-ghost", false);
    check_fixture("5-append", false);
    check_fixture("foreign", false);
    check_fixture("e2e", false);
    // the raw records: a null union branch decodes to null, Iceberg's maps come as arrays
    // of key/value records (logicalType is ignored)
    auto rows = read_all(slurp("m-1.avro"));
    CHECK_EQ(rows.size(), size_t(2));
    CHECK(rows[0]["data_file"]["column_sizes"].is_array());
    CHECK_EQ(rows[0]["data_file"]["column_sizes"][0]["key"].get<int>(), 1);
    CHECK(rows[0]["data_file"]["key_metadata"].is_null());
    CHECK(rows[0]["sequence_number"].is_null());
    CHECK_EQ(rows[0]["data_file"]["lower_bounds"][0]["value"].get_binary().size(), size_t(8));
}

TEST(tables_manifest_model_rejects_missing_fields) {
    // a manifest list whose records lack manifest_length
    std::string
        schema = R"({"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string"}]})";
    std::string rec;
    put_bytes(rec, "s3://b/m.avro");
    avro::Reader r(ocf(schema, "null", {rec}));
    CHECK_EQ(status_of([&] { parse_manifest_list(r, 10); }), 409);
    // v1-style manifest list without content / sequence numbers reads as zeros
    std::string v1 = R"({"type":"record","name":"manifest_file","fields":[
        {"name":"manifest_path","type":"string"},{"name":"manifest_length","type":"long"},
        {"name":"partition_spec_id","type":"int"},{"name":"added_snapshot_id","type":["null","long"]}]})";
    std::string rec1;
    put_bytes(rec1, "s3://b/m.avro");
    put_long(rec1, 1234);
    put_long(rec1, 0);
    put_long(rec1, 1);
    put_long(rec1, 77);
    avro::Reader r1(ocf(v1, "null", {rec1}));
    auto ms = parse_manifest_list(r1, 10);
    CHECK_EQ(ms.size(), size_t(1));
    CHECK_EQ(ms[0].length, 1234);
    CHECK_EQ(ms[0].content, 0);
    CHECK_EQ(ms[0].sequence_number, 0);
    CHECK_EQ(ms[0].added_snapshot_id, 77);
    // a manifest entry with a non-integer status
    std::string bad = R"({"type":"record","name":"manifest_entry","fields":[{"name":"status","type":"string"},
        {"name":"data_file","type":{"type":"record","name":"r2","fields":[{"name":"file_path","type":"string"}]}}]})";
    std::string rec2;
    put_bytes(rec2, "ADDED");
    put_bytes(rec2, "s3://b/f");
    avro::Reader r2(ocf(bad, "null", {rec2}));
    CHECK_EQ(status_of([&] { parse_manifest(r2, 10); }), 409);
}
