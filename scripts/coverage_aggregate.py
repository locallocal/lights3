#!/usr/bin/env python3
"""Aggregates gcov JSON documents (gcov --json-format --stdout, concatenated on
stdin) into per-file line coverage for the tree under argv[1] (docs/testing.md §7).

gcov lists every instrumented line per function, so template instantiations and
headers included from many translation units repeat lines: the union by
(file, line number) across all documents is the honest denominator. GCC's gcov
barely instruments coroutine bodies (only the ramp function), so coroutine-heavy
files under-report — gcovr/lcov share that limitation.
"""
import json
import sys


def main():
    root = sys.argv[1]
    inst, hit = {}, {}
    dec = json.JSONDecoder()
    buf = sys.stdin.read()
    pos = 0
    while pos < len(buf):
        while pos < len(buf) and buf[pos].isspace():
            pos += 1
        if pos >= len(buf):
            break
        try:
            doc, end = dec.raw_decode(buf, pos)
        except ValueError:
            break
        pos = end
        for f in doc.get("files", []):
            name = f.get("file", "")
            if not name.startswith(root) or "third_party" in name:
                continue
            i = inst.setdefault(name, set())
            h = hit.setdefault(name, set())
            for ln in f.get("lines", []):
                i.add(ln["line_number"])
                if ln.get("count", 0) > 0:
                    h.add(ln["line_number"])
    ti = th = 0
    for name in sorted(inst):
        n, k = len(inst[name]), len(hit[name])
        if n == 0:
            continue  # header with declarations only
        ti += n
        th += k
        print(f"{100.0 * k / n:6.1f}% of {n:5d}  {name[len(root):]}")
    if ti:
        print(f"TOTAL src/: {100.0 * th / ti:.1f}% of {ti} instrumented lines")
    else:
        print("no coverage data found (was the build configured with --coverage?)")


if __name__ == "__main__":
    main()
