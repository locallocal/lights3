// roadmap §6.1: fault injection — the facade (spec grammar, one-shot / sticky
// points, reset) and the wired points on the localfs and duostore pack IO paths:
// a fault surfaces as InternalError with the injected errno text, leaves no
// half-written state behind, and the backend keeps working once the point clears
#include <cerrno>
#include <filesystem>
#include <fstream>

#include "core/fault.h"
#include "core/thread_pool.h"
#include "storage/localfs/localfs_backend.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif

using namespace lights3;
using namespace lights3::storage;
using namespace backend_suite;
using lights3::s3::S3Error;
using lights3::s3::S3ErrorCode;
namespace fs = std::filesystem;

namespace {

struct FaultReset {
    ~FaultReset() { fault::reset(); }
};

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

template <class F>
bool throws(F&& fn) {
    try {
        fn();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

// Every point declared in fault::kPoints must appear in the sources (a point that
// exists only in the table would be a documented fault nobody can trigger)
size_t count_files_with(const fs::path& dir, const std::string& needle) {
    size_t n = 0;
    for (auto& e : fs::recursive_directory_iterator(dir)) {
        if (!e.is_regular_file()) continue;
        auto ext = e.path().extension();
        if (ext != ".cc" && ext != ".h") continue;
        std::ifstream in(e.path());
        std::string text((std::istreambuf_iterator<char>(in)), {});
        if (text.find("fault::check(\"" + needle + "\")") != std::string::npos) ++n;
    }
    return n;
}

}  // namespace

TEST(fault_spec_grammar_and_countdown) {
    FaultReset guard;
    CHECK_EQ(fault::check("localfs.write"), 0);  // nothing armed: the fast path
    fault::arm("localfs.write:2:ENOSPC, duostore.pack.pwrite");
    CHECK_EQ(fault::describe(), "duostore.pack.pwrite:1:" + std::to_string(EIO) +
                                    ", localfs.write:2:" + std::to_string(ENOSPC));
    CHECK_EQ(fault::check("localfs.write"), ENOSPC);
    CHECK_EQ(fault::check("localfs.rename"), 0);  // not armed
    CHECK_EQ(fault::check("localfs.write"), ENOSPC);
    CHECK_EQ(fault::check("localfs.write"), 0);  // count exhausted, point cleared
    CHECK_EQ(fault::check("duostore.pack.pwrite"), EIO);
    CHECK_EQ(fault::check("duostore.pack.pwrite"), 0);
    CHECK_EQ(fault::describe(), "");
    fault::arm("localfs.fsync:0:5");  // 0 = sticky, numeric errno
    for (int i = 0; i < 5; ++i) CHECK_EQ(fault::check("localfs.fsync"), 5);
    fault::reset();
    CHECK_EQ(fault::check("localfs.fsync"), 0);
    // Malformed specs are refused as a whole
    CHECK(throws([] { fault::arm("nosuch.point"); }));
    CHECK(throws([] { fault::arm("localfs.write:x"); }));
    CHECK(throws([] { fault::arm("localfs.write:1:EWHATEVER"); }));
    CHECK_EQ(fault::describe(), "");
}

TEST(fault_points_are_wired_in_sources) {
    fs::path src = fs::path(__FILE__).parent_path().parent_path().parent_path() / "src";
    if (!fs::exists(src)) return;  // installed-tree run: nothing to cross-check
    for (auto p : fault::kPoints)
        if (count_files_with(src, std::string(p)) == 0)
            throw mini_test::Failure("fault point not wired anywhere: " + std::string(p));
}

TEST(fault_localfs_write_rename_fsync) {
    FaultReset guard;
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto b = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "before", "ok");

    // EIO on the staging write: InternalError carrying the errno text, no object,
    // and the staging tmp file was cleaned up
    fault::arm("localfs.write:1:EIO");
    try {
        put(*b, "bkt", "k", "payload");
        throw mini_test::Failure("put should have failed under localfs.write");
    } catch (const S3Error& e) {
        CHECK_EQ(int(e.code), int(S3ErrorCode::InternalError));
        CHECK(contains(e.message, "Input/output error"));
    }
    CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "k")), S3ErrorCode::NoSuchKey);
    size_t staged = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "staging"))
        if (e.is_regular_file()) ++staged;
    CHECK_EQ(staged, size_t(0));
    // One-shot: the retry succeeds and the earlier object is untouched
    CHECK(!put(*b, "bkt", "k", "payload").etag.empty());
    CHECK_EQ(read_all(*sync_wait(b->get_object("bkt", "k", std::nullopt)).body), "payload");
    CHECK_EQ(read_all(*sync_wait(b->get_object("bkt", "before", std::nullopt)).body), "ok");

    // Sticky ENOSPC on the commit rename: every write fails until reset, reads keep working
    fault::arm("localfs.rename:0:ENOSPC");
    for (int i = 0; i < 2; ++i) {
        try {
            put(*b, "bkt", "k2", "x");
            throw mini_test::Failure("put should have failed under localfs.rename");
        } catch (const S3Error& e) {
            CHECK_EQ(int(e.code), int(S3ErrorCode::InternalError));
            CHECK(contains(e.message, "No space left on device"));
        }
    }
    CHECK_EQ(read_all(*sync_wait(b->get_object("bkt", "k", std::nullopt)).body), "payload");
    fault::reset();
    CHECK(!put(*b, "bkt", "k2", "x").etag.empty());

    // fsync failure is a write failure too (durability is part of the 200)
    fault::arm("localfs.fsync:1:EIO");
    bool failed = false;
    try {
        put(*b, "bkt", "k3", "y");
    } catch (const S3Error& e) {
        failed = e.code == S3ErrorCode::InternalError;
    }
    CHECK(failed || !fsutil::fsync_enabled());  // LIGHTS3_FSYNC=0 makes the point unreachable
    fault::reset();
    CHECK(!put(*b, "bkt", "k3", "y").etag.empty());
    sync_wait(b->close());
}

#ifdef LIGHTS3_DUOSTORE
TEST(fault_duostore_pack_pwrite_and_fdatasync) {
    FaultReset guard;
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig cfg;
    cfg.name = "t";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.meta_sync = false;
    auto b = std::make_shared<DuoStoreBackend>(std::move(cfg), pool);
    sync_wait(b->create_bucket("bkt"));
    put(*b, "bkt", "before", "small");  // small objects land in packs (default threshold)

    fault::arm("duostore.pack.pwrite:1:EIO");
    try {
        put(*b, "bkt", "k", "small payload");
        throw mini_test::Failure("put should have failed under duostore.pack.pwrite");
    } catch (const S3Error& e) {
        CHECK_EQ(int(e.code), int(S3ErrorCode::InternalError));
        CHECK(contains(e.message, "pwrite pack record"));
    }
    CHECK_THROWS_S3(sync_wait(b->head_object("bkt", "k")), S3ErrorCode::NoSuchKey);
    CHECK(!put(*b, "bkt", "k", "small payload").etag.empty());
    CHECK_EQ(read_all(*sync_wait(b->get_object("bkt", "k", std::nullopt)).body), "small payload");

    fault::arm("duostore.pack.fdatasync:1:ENOSPC");
    try {
        put(*b, "bkt", "k2", "another");
        throw mini_test::Failure("put should have failed under duostore.pack.fdatasync");
    } catch (const S3Error& e) {
        CHECK_EQ(int(e.code), int(S3ErrorCode::InternalError));
        CHECK(contains(e.message, "fdatasync pack"));
    }
    CHECK(!put(*b, "bkt", "k2", "another").etag.empty());
    // Earlier data is intact after both faults
    CHECK_EQ(read_all(*sync_wait(b->get_object("bkt", "before", std::nullopt)).body), "small");
    CHECK_EQ(read_all(*sync_wait(b->get_object("bkt", "k", std::nullopt)).body), "small payload");
    sync_wait(b->close());
}
#endif
