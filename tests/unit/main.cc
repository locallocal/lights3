#include "unit/mini_test.h"

int main(int argc, char** argv) {
    // 带参 = 子进程模式（崩溃注入测试经 execv 自身进入，见 mini_test.h）
    if (argc > 1) return mini_test::run_child(argc, argv);
    return mini_test::run_all();
}
