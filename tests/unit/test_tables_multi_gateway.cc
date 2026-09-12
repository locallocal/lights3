// The two-gateway S3 Tables suite over MemoryBackend (docs/s3-tables-design.md §5.5):
// two MemoryBackends cannot share a medium, so both gateways hold the same instance
// -- two catalog stacks over one PutCondition CAS is what the convergence and recovery
// claims rest on. The redis / tikv variants live in test_duostore_redis.cc / _tikv.cc
#include "storage/memory/memory_backend.h"
#include "unit/tables_multi_gateway_suite.h"

TEST(tables_multi_gateway_memory) {
    auto backend = std::make_shared<lights3::storage::MemoryBackend>();
    tables_multi_gateway_suite::run(backend, backend);
}
