# Developer conveniences around the CMake build (build.sh drives the actual builds).
#
#   make format        reformat every C++ source and header in place (.clang-format)
#   make format-check  list files that `make format` would change; exit 1 if any
#   make help          this text
#
# CLANG_FORMAT selects the binary (a pinned major version keeps the output stable
# across machines: the Google preset drifts slightly between LLVM releases).

CLANG_FORMAT ?= clang-format
# Tracked C++ sources under src/ and tests/ (git ls-files honours .gitignore, so
# build-*/ never leaks in; third_party/ submodules carry their own style).
CXX_SOURCES := $(shell git ls-files -- 'src/*.h' 'src/*.cc' 'tests/*.h' 'tests/*.cc')

.PHONY: help format format-check

help:
	@sed -n '3,5p' $(MAKEFILE_LIST) | sed 's/^#   //'

format:
	@echo "$(CXX_SOURCES)" | tr ' ' '\n' | xargs -P "$$(nproc)" -n 32 $(CLANG_FORMAT) -i --style=file
	@echo "formatted $(words $(CXX_SOURCES)) files"

format-check:
	@status=0; \
	for f in $(CXX_SOURCES); do \
	    if ! $(CLANG_FORMAT) --style=file --dry-run -Werror "$$f" >/dev/null 2>&1; then \
	        echo "needs formatting: $$f"; status=1; \
	    fi; \
	done; \
	[ $$status -eq 0 ] && echo "format-check: $(words $(CXX_SOURCES)) files clean"; \
	exit $$status
