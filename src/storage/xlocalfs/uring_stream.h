// L3: multi-in-flight IO streams over UringEngine (roadmap §3.4 ①②③).
//
// The engine's original discipline was "at most one in-flight op per coroutine frame",
// which made destruction trivially safe but left queue_depth unused within a single
// request. These streams re-establish that safety under multiple in-flight ops with one
// ownership rule: all IO state (block buffers, the fd, the fixed-file slot) lives in a
// heap-allocated, intrusively refcounted block; every in-flight op holds one reference.
// Destroying a stream mid-flight (client disconnect, exception unwinding) therefore never
// frees memory the kernel is still writing into -- the abandoned ops complete naturally,
// drop their references, and the last one releases the block (returning fixed buffers,
// unregistering the file slot, closing the fd) on the reaper/pool side. No per-op
// cancellation is issued; disk ops complete in bounded time and engine shutdown's drain
// already waits for them.
//
// Blocks come from the ring's registered fixed-buffer pool when available
// (READ_FIXED/WRITE_FIXED, roadmap §3.4 ②) and silently fall back to heap blocks with
// plain READ/WRITE when the pool is exhausted or registration failed. Streams whose
// expected size crosses kFixedFileMinBytes also register their fd in the ring's fixed
// file table for the duration of the stream.
#pragma once

#include <cstdint>
#include <optional>
#include <span>

#include "core/task.h"
#include "storage/xlocalfs/uring.h"

namespace lights3::storage {

namespace uring_detail {
struct StreamState;
}

// Read-ahead streaming reader (roadmap §3.4 ①): keeps up to read_depth block reads in
// flight past the consumer's position, so a serial consumer (HTTP response pump) overlaps
// disk latency instead of paying one full CQE round trip per block. read() copies from the
// completed block into the caller's buffer (a 64KiB memcpy is noise next to the saved
// round trip, and it is what lets the blocks be registered fixed buffers).
// EOF semantics match FdStreamReader: a short/zero read (file truncated externally) ends
// the stream early; read errors throw S3Error(InternalError).
class UringReadStream {
public:
    // Reads [off, off+len) from fd. own_fd: the stream closes fd once the last reference
    // (including abandoned in-flight ops) is gone
    UringReadStream(std::shared_ptr<UringEngine> eng, int fd, uint64_t off, uint64_t len,
                    bool own_fd = true);
    ~UringReadStream();
    UringReadStream(const UringReadStream&) = delete;

    Task<size_t> read(std::span<std::byte> out);  // 0 = EOF
    uint64_t remaining() const { return remaining_; }

private:
    void fill();  // submit read-ahead into free slots

    uring_detail::StreamState* st_;
    uint64_t remaining_;
    unsigned head_ = 0;     // slot index the consumer reads next
    unsigned inflight_ = 0; // slots submitted and not yet consumed (ring order from head_)
    bool done_ = false;
};

// Pipelined streaming writer: acquire() hands out a free block, the caller fills it
// (typically by reading the request body straight into it) and commit()s; the write SQE
// for block k is pushed when block k+1 is committed ("hold-back"), so the final block is
// still un-submitted when finish() runs and can be chained WRITE -> linked FSYNC in a
// single submission (roadmap §3.4 ③) -- the common small object costs one submission and
// one wakeup for write+persist. Blocks already pushed overlap with receiving the next body
// block, up to write_depth in flight.
// finish() is the only place results are fully settled; a stream destroyed without
// finish() abandons its writes (the temp file is being discarded anyway).
class UringWriteStream {
public:
    // Appends at off. The stream dup()s fd: under SQPOLL the kernel resolves an SQE's fd
    // only when the poll thread picks it up, so the caller closing its fd (TmpFile
    // unwinding) must not invalidate abandoned in-flight writes. expected_len (when known)
    // gates the fixed-file registration
    UringWriteStream(std::shared_ptr<UringEngine> eng, int fd, uint64_t off,
                     std::optional<uint64_t> expected_len = std::nullopt);
    ~UringWriteStream();
    UringWriteStream(const UringWriteStream&) = delete;

    // A free block of options().block_size bytes; waits for (and checks) the oldest
    // in-flight write when everything is busy. Write errors surface here or in finish()
    Task<std::span<std::byte>> acquire();
    // Queue the first n bytes of the acquired block for writing at the current offset
    // (n=0 just returns the block). Pushes the previously held block to the ring
    void commit(size_t n);
    // Drain all writes, then submit the held final block -- linked to an FSYNC
    // (fdatasync semantics) when `fdatasync` is set and the kernel supports links --
    // and settle every result. Short writes are transparently resubmitted; a linked
    // fsync cancelled by a short write is retried standalone. When the kernel lacks
    // IORING_OP_FSYNC the fallback is a blocking fdatasync (we are on a pool thread).
    // -EINVAL from fsync is tolerated (filesystem does not support it)
    Task<void> finish(bool fdatasync);
    uint64_t written() const { return written_; }

private:
    Task<void> settle_oldest();  // wait for the oldest in-flight write, resubmit short writes

    uring_detail::StreamState* st_;
    uint64_t off_;
    uint64_t written_ = 0;
    int held_ = -1;     // slot handed out by acquire(), not yet committed
    int pending_ = -1;  // committed but deliberately not yet pushed (finish() links it)
    bool finished_ = false;
};

}  // namespace lights3::storage
