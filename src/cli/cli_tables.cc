#include "cli/cli_tables.h"

#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/task.h"
#include "tables/object_catalog_store.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#include "tables/duo_meta_catalog_store.h"
#endif
#include "tables/table_bucket_store.h"

namespace lights3_cli {

namespace {

using nlohmann::json;

// The catalog store the configuration names (docs/s3-tables-design.md §12): the
// object backing on the default backend's .sys, or the duostore meta KV facade. The
// lights3 daemon assembles the same pair in Application::start_server
std::shared_ptr<lights3::tables::ITableCatalogStore> open_store(lights3::Application& app, const std::string& backing) {
    using namespace lights3;
    const auto& cfg = app.config();
    auto it = app.backends().find(cfg.buckets.default_backend);
    if (it == app.backends().end())
        throw std::runtime_error("tables: default backend '" + cfg.buckets.default_backend + "' is not built");
    if (backing == "duostore") {
#ifdef LIGHTS3_DUOSTORE
        auto* duo = dynamic_cast<storage::DuoStoreBackend*>(it->second.get());
        if (!duo) throw std::runtime_error("tables: catalog_backing duostore needs a duostore default backend");
        return std::make_shared<tables::DuoMetaCatalogStore>(duo->meta());
#else
        throw std::runtime_error("tables: catalog_backing duostore needs a build with duostore");
#endif
    }
    return std::make_shared<tables::ObjectCatalogStore>(it->second);
}

std::string file_arg(const Cmd& c) {
    std::string file = c->var<std::string>("file");
    if (c->args().size() > 1) {
        g_exit = 2;
        throw std::runtime_error("tables " + c->name() + ": too many arguments");
    }
    if (c->args().size() == 1) file = c->args().front();
    if (file.empty()) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error("tables " + c->name() + ": <file> is required");
    }
    return file;
}

// --backing overrides tables.catalog_backing (the migration reads one, writes the other)
std::string backing_of(const Cmd& c, const lights3::Config& cfg) {
    std::string b = c->var<std::string>("backing");
    if (b.empty()) b = cfg.tables.catalog_backing;
    if (b != "object" && b != "duostore") {
        g_exit = 2;
        throw std::runtime_error("tables: --backing must be object or duostore");
    }
    return b;
}

// JSON lines: {"bucket": "...", "key": "<catalog key>", "body": <the object, as JSON>}
void run_export(const Cmd& c) {
    using namespace lights3;
    std::string file = file_arg(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    std::string backing = backing_of(c, app.config());
    auto store = open_store(app, backing);
    auto markers = sync_wait(tables::TableBucketStore::load(app.backends().at(app.config().buckets.default_backend)));
    std::ofstream out(file, std::ios::trunc);
    if (!out) throw std::runtime_error("tables export: cannot write " + file);
    size_t entries = 0, buckets = 0;
    for (auto& [bucket, marker] : *markers->snapshot()) {
        if (!marker.enabled) continue;
        ++buckets;
        for (auto& e : sync_wait(store->export_raw(bucket))) {
            json line;
            line["bucket"] = bucket;
            line["key"] = e.key;
            line["body"] = json::parse(e.body, nullptr, false);
            if (line["body"].is_discarded()) line["body"] = e.body;
            out << line.dump() << '\n';
            ++entries;
        }
    }
    out.close();
    LOG_INFO("tables export ({}): {} table bucket(s), {} catalog object(s) -> {}", backing, buckets, entries, file);
    app.shutdown();
}

void run_import(const Cmd& c) {
    using namespace lights3;
    std::string file = file_arg(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    std::string backing = backing_of(c, app.config());
    auto store = open_store(app, backing);
    std::ifstream in(file);
    if (!in) throw std::runtime_error("tables import: cannot read " + file);
    size_t entries = 0;
    for (std::string line; std::getline(in, line);) {
        if (line.empty()) continue;
        json j = json::parse(line, nullptr, false);
        if (!j.is_object() || !j.contains("bucket") || !j.contains("key") || !j.contains("body"))
            throw std::runtime_error("tables import: malformed line " + std::to_string(entries + 1));
        tables::RawEntry e;
        e.key = j["key"].get<std::string>();
        e.body = j["body"].is_string() ? j["body"].get<std::string>() : j["body"].dump();
        sync_wait(store->import_raw(j["bucket"].get<std::string>(), e));
        ++entries;
    }
    LOG_INFO("tables import ({}): {} catalog object(s) <- {}", backing, entries, file);
    app.shutdown();
}

Cmd make_leaf(const char* name, const char* example, const char* usage, const char* help_long, const char* help_short,
              void (*run)(const Cmd&)) {
    auto cmd = std::make_shared<ccmd::command>(name, example, usage, help_long, help_short, run);
    add_config_flag(cmd);
    cmd->var<std::string>("file", "", "JSON-lines file (alternative to the positional)");
    cmd->var<std::string>("backing", "",
                          "catalog backing to read / write: object | duostore (default: tables.catalog_backing)");
    return cmd;
}

}  // namespace

Cmd make_tables() {
    auto cmd = make_group("tables", "lights3 tables export catalog.jsonl --config=config/lights3.yaml",
                          "lights3 tables <export|import> <file> [--backing=object|duostore] [--config=<path>]",
                          "S3 Tables catalog state migration between the object backing (.sys of the default "
                          "backend) and the duostore meta backing (tables.catalog_backing, "
                          "docs/s3-tables-design.md §12): export writes every catalog object of every "
                          "table bucket as JSON lines, import writes them back. Offline: the backends are "
                          "built, no server listens; stop the gateways first (the catalog must not move "
                          "while it is copied). Table-bucket markers stay in .sys on both backings.",
                          "S3 Tables catalog export / import (backing migration)");
    cmd->add_subcommand(make_leaf("export", "lights3 tables export catalog.jsonl",
                                  "lights3 tables export <file> [--backing=object|duostore] [--config=<path>]",
                                  "Write the catalog state of every table bucket to <file> as JSON lines.",
                                  "export the catalog to a file", run_export));
    cmd->add_subcommand(make_leaf("import", "lights3 tables import catalog.jsonl --backing=duostore",
                                  "lights3 tables import <file> [--backing=object|duostore] [--config=<path>]",
                                  "Write the JSON lines of <file> into the catalog backing (existing keys are "
                                  "overwritten).",
                                  "import the catalog from a file", run_import));
    return cmd;
}

}  // namespace lights3_cli
