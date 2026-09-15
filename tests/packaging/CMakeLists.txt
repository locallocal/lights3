# Install tree + --version (docs/usage/deployment.md): cmake --install into a
# scratch prefix, layout / unit relocation / config preservation / version output
add_test(NAME install_tree
         COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/check_install.sh ${PROJECT_BINARY_DIR})
