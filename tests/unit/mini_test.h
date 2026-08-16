// Minimal test framework: no gtest in this environment, so a registry + assertion macros cover unit-test needs
#pragma once

#include <cstdio>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace mini_test {

struct TestCase {
    const char* name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(const char* name, std::function<void()> fn) {
        registry().push_back({name, std::move(fn)});
    }
};

struct Failure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

template <class A, class B>
void check_eq(const A& a, const B& b, const char* ea, const char* eb, const char* file,
              int line) {
    if (!(a == b)) {
        std::ostringstream os;
        os << file << ":" << line << " CHECK_EQ(" << ea << ", " << eb << ") failed: '" << a
           << "' != '" << b << "'";
        throw Failure(os.str());
    }
}

inline int run_all() {
    int failed = 0;
    for (auto& t : registry()) {
        try {
            t.fn();
            printf("[ OK ] %s\n", t.name);
        } catch (const std::exception& e) {
            printf("[FAIL] %s\n       %s\n", t.name, e.what());
            ++failed;
        }
    }
    printf("%zu tests, %d failed\n", registry().size(), failed);
    return failed == 0 ? 0 : 1;
}

// ---- Child-process mode (for crash injection) ----
// The test process restarts itself with arguments (execv /proc/self/exe <mode> ...) to enter a registered child routine;
// the child routine ends with _exit / being SIGKILLed to simulate kill -9; the parent reopens the state directory and verifies convergence

using ChildFn = int (*)(int argc, char** argv);

struct ChildCase {
    const char* name;
    ChildFn fn;
};

inline std::vector<ChildCase>& child_registry() {
    static std::vector<ChildCase> r;
    return r;
}

struct ChildRegistrar {
    ChildRegistrar(const char* name, ChildFn fn) { child_registry().push_back({name, fn}); }
};

inline int run_child(int argc, char** argv) {  // argv[1] = child mode name
    for (auto& c : child_registry())
        if (std::string(argv[1]) == c.name) return c.fn(argc, argv);
    fprintf(stderr, "unknown child mode: %s\n", argv[1]);
    return 2;
}

}  // namespace mini_test

#define TEST(name)                                                          \
    static void test_fn_##name();                                          \
    static ::mini_test::Registrar reg_##name(#name, test_fn_##name);       \
    static void test_fn_##name()

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::ostringstream os_;                                                       \
            os_ << __FILE__ << ":" << __LINE__ << " CHECK(" #cond ") failed";             \
            throw ::mini_test::Failure(os_.str());                                        \
        }                                                                                  \
    } while (0)

#define CHECK_EQ(a, b) ::mini_test::check_eq((a), (b), #a, #b, __FILE__, __LINE__)

#define CHECK_THROWS_S3(expr, expected_code)                                               \
    do {                                                                                   \
        bool thrown_ = false;                                                              \
        try {                                                                              \
            expr;                                                                          \
        } catch (const ::lights3::s3::S3Error& e_) {                                       \
            thrown_ = true;                                                                \
            CHECK_EQ(::lights3::s3::wire_code(e_.code),                                    \
                     ::lights3::s3::wire_code(expected_code));                             \
        }                                                                                  \
        if (!thrown_) {                                                                    \
            std::ostringstream os_;                                                       \
            os_ << __FILE__ << ":" << __LINE__ << " expected S3Error " #expected_code;    \
            throw ::mini_test::Failure(os_.str());                                        \
        }                                                                                  \
    } while (0)
