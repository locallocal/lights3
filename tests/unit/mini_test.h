// 微型测试框架：环境无 gtest，用注册表 + 断言宏覆盖单测需求
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
