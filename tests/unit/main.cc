#include "unit/mini_test.h"

int main(int argc, char** argv) {
    // With arguments = child-process mode (crash-injection tests re-enter via execv of self, see mini_test.h)
    if (argc > 1) return mini_test::run_child(argc, argv);
    return mini_test::run_all();
}
