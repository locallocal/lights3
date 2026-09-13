// Streaming MD5 for the request-body write loops (docs/architecture/http-adapter.md §2.4 ⑬).
// Every backend's PUT / UploadPart loop used to run read -> md5.update -> write
// serially on one thread, so a 4 MiB PUT spent ~3.3 ms of its ~7 ms in MD5
// alone (OpenSSL MD5 is ~1.26 GB/s per stream and cannot be parallelised).
// PipelinedMd5 hashes chunk k on a pool thread while the caller writes chunk k
// and reads chunk k+1:
//
//   feed(k)  awaits the hash of chunk k-1 (digest order; the caller's other
//            buffer is free again), then starts chunk k's hash and returns
//   final_hex() awaits the last chunk and finalises
//
// Contract: the caller alternates two buffers, and the bytes handed to feed()
// stay untouched until the *next* feed() (or final_hex()) returned -- i.e. read
// chunk k+2 into buffer (k mod 2) only after feed(k+1) returned. Bodies below
// kInlineBytes are hashed inline so small objects never pay a pool hop; after
// the threshold every chunk goes to the pool (the digest state is only ever
// touched by one thread at a time either way). A co_await on feed() may
// resume the caller on the pool thread that finished the hash: the drivers'
// body readers switch back to their own thread before blocking (the same
// junction rule as everywhere else), and the loops' blocking writes are pool
// work anyway.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "core/task.h"
#include "core/thread_pool.h"
#include "core/util/crypto.h"

namespace lights3::storage {

class PipelinedMd5 {
public:
    // Chunk size the write loops read with: 128 KiB halves the per-chunk hop
    // count of the previous 64 KiB stack buffers for the same memory order
    static constexpr size_t kChunk = 128 * 1024;
    // Hashed inline before pipelining starts
    static constexpr uint64_t kInlineBytes = 256 * 1024;

    // pool == nullptr hashes everything inline (assemblies without a pool)
    explicit PipelinedMd5(ThreadPool* pool) : pool_(pool) {}
    PipelinedMd5(const PipelinedMd5&) = delete;
    PipelinedMd5& operator=(const PipelinedMd5&) = delete;

    // Two kChunk buffers for the alternating read; the caller indexes them with
    // a toggling bit. Recycled through a small per-thread cache and never
    // cleared: a fresh 256 KiB allocation per PUT is an mmap/munmap pair plus
    // page faults, and make_unique<T[]> would memset it too -- measured as a
    // ~10% loss on 16 KiB PUTs. Returning on another thread is fine (a cache)
    struct Recycle {
        void operator()(std::byte* p) const noexcept {
            auto& c = cache();
            if (c.size() < kCacheCap)
                c.push_back(p);
            else
                delete[] p;
        }
    };
    using Buffers = std::unique_ptr<std::byte[], Recycle>;
    static Buffers make_buffers() {
        auto& c = cache();
        if (!c.empty()) {
            std::byte* p = c.back();
            c.pop_back();
            return Buffers(p);
        }
        // default-init: no memset
        return Buffers(new std::byte[2 * kChunk]);
    }

    Task<void> feed(std::span<const std::byte> chunk) {
        if (pending_.pending()) co_await pending_;
        if (!pool_ || total_ < kInlineBytes)
            update(chunk);
        else
            pending_.start(hash_on_pool(chunk));
        total_ += chunk.size();
    }

    Task<std::string> final_hex() {
        if (pending_.pending()) co_await pending_;
        co_return md5_.final_hex();
    }

    uint64_t total() const { return total_; }

private:
    Task<void> hash_on_pool(std::span<const std::byte> chunk) {
        co_await pool_->schedule();
        update(chunk);
    }
    void update(std::span<const std::byte> chunk) {
        md5_.update(std::span(reinterpret_cast<const uint8_t*>(chunk.data()), chunk.size()));
    }

    static constexpr size_t kCacheCap = 8;
    static std::vector<std::byte*>& cache() {
        thread_local std::vector<std::byte*> c;
        return c;
    }

    ThreadPool* pool_;
    util::HashStream md5_{util::HashStream::Algo::Md5};
    Started<void> pending_;
    uint64_t total_ = 0;
};

}  // namespace lights3::storage
