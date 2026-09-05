// Standalone entry for the fuzz harnesses (roadmap §6.1): without libFuzzer
// (GCC builds, the default), each harness links this main and replays every file
// under the directories given on the command line — the checked-in seed corpus
// becomes a crash-regression test (ctest fuzz_regression). With
// -DLIGHTS3_FUZZ_LIBFUZZER=ON (clang) the harness links -fsanitize=fuzzer and
// this file is not compiled: libFuzzer supplies main and mutates the corpus.
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size);

int main(int argc, char** argv) {
    namespace fs = std::filesystem;
    size_t inputs = 0;
    for (int i = 1; i < argc; ++i) {
        std::vector<fs::path> files;
        fs::path p(argv[i]);
        if (fs::is_directory(p)) {
            for (auto& e : fs::recursive_directory_iterator(p))
                if (e.is_regular_file()) files.push_back(e.path());
        } else {
            files.push_back(p);
        }
        for (auto& f : files) {
            std::ifstream in(f, std::ios::binary);
            std::string bytes((std::istreambuf_iterator<char>(in)), {});
            LLVMFuzzerTestOneInput(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
            ++inputs;
        }
    }
    // A synthetic sweep on top of the corpus: single bytes and short runs so an
    // empty corpus still exercises the tiny-input edge cases
    for (int b = 0; b < 256; ++b) {
        uint8_t one = static_cast<uint8_t>(b);
        LLVMFuzzerTestOneInput(&one, 1);
        uint8_t run[4] = {one, one, one, one};
        LLVMFuzzerTestOneInput(run, sizeof(run));
    }
    LLVMFuzzerTestOneInput(nullptr, 0);
    std::printf("fuzz regression: %zu corpus inputs + synthetic sweep, no crash\n", inputs);
    return 0;
}
