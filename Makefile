# Developer conveniences around the CMake build (build.sh drives the actual builds).
#
#   make release       Release build in $(RELEASE_DIR) (build-rel) via build.sh
#   make debug         Debug build in $(DEBUG_DIR) (build) via build.sh
#   make package       release, then CPack in $(RELEASE_DIR): deb / rpm / tgz under packages/
#   make clean         remove $(RELEASE_DIR) and $(DEBUG_DIR) (other build-* variants are kept)
#   make format        move trailing comments above their code, then clang-format in place
#   make format-check  list trailing comments and files clang-format would change; exit 1 if any
#   make help          this text
#
# JOBS (default: half the cores) is the build parallelism; BUILD_ARGS passes extra
# build.sh flags (e.g. BUILD_ARGS="--redis --sqlite"); RELEASE_DIR / DEBUG_DIR move
# the build directories.  CLANG_FORMAT selects the formatter binary (a pinned major
# version keeps the output stable across machines: the Google preset drifts slightly
# between LLVM releases).

JOBS        ?= $(shell j=$$(( $$(nproc) / 2 )); echo $$(( j > 0 ? j : 1 )))
BUILD_ARGS  ?=
RELEASE_DIR ?= build-rel
DEBUG_DIR   ?= build
CLANG_FORMAT ?= clang-format
# Tracked C++ sources under src/ and tests/ (git ls-files honours .gitignore, so
# build-*/ never leaks in; third_party/ submodules carry their own style).
CXX_SOURCES := $(shell git ls-files -- 'src/*.h' 'src/*.cc' 'tests/*.h' 'tests/*.cc')

# House rule beyond clang-format (scripts/check_comments.py): a `//` comment sits on
# its own line above the code, never at the end of a code line — except the closers
# Google style asks for (`}  // namespace x`, `#endif  // GUARD`).
COMMENT_LINT := python3 scripts/check_comments.py

.PHONY: help release debug package clean format format-check

help:
	@sed -n '3,9p' $(MAKEFILE_LIST) | sed 's/^#   //'

# build.sh initialises the submodules, configures (Ninja when present) and compiles;
# the build type is sticky in the CMake cache, so each target owns its directory.
release:
	./build.sh -B $(RELEASE_DIR) -DCMAKE_BUILD_TYPE=Release -j $(JOBS) $(BUILD_ARGS)

debug:
	./build.sh --debug -B $(DEBUG_DIR) -j $(JOBS) $(BUILD_ARGS)

# CPack picks the generators available on the host (cmake/Packaging.cmake): DEB when
# dpkg-deb is present, RPM when rpmbuild is, TGZ otherwise; output in
# $(RELEASE_DIR)/packages/.  Packages are always built from the Release tree.
package: release
	cmake --build $(RELEASE_DIR) --target package -j $(JOBS)
	@find $(RELEASE_DIR)/packages -maxdepth 1 -type f | sort

clean:
	rm -rf $(RELEASE_DIR) $(DEBUG_DIR)

format:
	@$(COMMENT_LINT) --fix $(CXX_SOURCES)
	@echo "$(CXX_SOURCES)" | tr ' ' '\n' | xargs -P "$$(nproc)" -n 32 $(CLANG_FORMAT) -i --style=file
	@echo "formatted $(words $(CXX_SOURCES)) files"

format-check:
	@status=0; \
	$(COMMENT_LINT) $(CXX_SOURCES) || status=1; \
	for f in $(CXX_SOURCES); do \
	    if ! $(CLANG_FORMAT) --style=file --dry-run -Werror "$$f" >/dev/null 2>&1; then \
	        echo "needs formatting: $$f"; status=1; \
	    fi; \
	done; \
	[ $$status -eq 0 ] && echo "format-check: $(words $(CXX_SOURCES)) files clean"; \
	exit $$status
