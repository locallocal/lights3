#!/usr/bin/env python3
"""Trailing-comment lint for the C++ tree (Makefile: `make format-check` / `make format`).

House rule: a `//` comment lives on its own line above the code it describes, never
at the end of a code line — clang-format keeps a trailing comment glued to its
statement, which limits how the code may wrap and makes the comment vanish in
diffs.  The only trailing comments allowed are the ones the Google style itself
asks for or that name what a line closes:

  }  // namespace foo          `#endif  // GUARD` / `#else  // ...`
  }  // <anything>             (a bare closing line: only braces / parens / `;`)
  // clang-format off|on       // NOLINT...                // fallthrough

Without arguments every violation is printed as file:line and the exit status is 1
when there is any.  With --fix the comments are moved:

  * a line ending in `{` — the comment describes the block: first line inside it;
  * a list element (line ending in `,`), a label (`case X:` / `public:`) or a
    preprocessor line: the line above itself;
  * any other statement: above the line that starts the statement (walking back
    over wrapped continuation lines), at that line's indentation;
  * pure-comment continuation lines aligned under the trailing comment travel
    with it.

Run `make format` afterwards: clang-format re-wraps the moved comments.
"""
import re
import sys

ALLOWED_TAIL = re.compile(r'^//\s*(namespace\b|clang-format (on|off)|NOLINT|fallthrough\b|FALLTHROUGH\b)')
BARE_CLOSER = re.compile(r'^[\s})\]]*;?\s*$')
PREPROC_KEEP = re.compile(r'^\s*#\s*(endif|else|elif)\b')
LABEL = re.compile(r'^\s*(case\b.*|default|public|private|protected)\s*:\s*$')


def comment_starts(text):
    """(line index, column) of every `//` that begins a line comment outside string,
    character and raw-string literals and block comments."""
    out = []
    i, n, line, col0 = 0, len(text), 0, 0
    state = 'code'
    raw_end = ''
    while i < n:
        c = text[i]
        if c == '\n':
            line += 1
            col0 = i + 1
            if state in ('line',):
                state = 'code'
            i += 1
            continue
        if state == 'code':
            if c == '/' and text.startswith('//', i):
                out.append((line, i - col0))
                state = 'line'
            elif c == '/' and text.startswith('/*', i):
                state = 'block'
                i += 1
            elif c == '"':
                m = re.match(r'R"([^(\s"\\]{0,16})\(', text[i - 1:i + 20]) if i > 0 and text[i - 1] == 'R' else None
                if m:
                    raw_end = ')' + m.group(1) + '"'
                    state = 'raw'
                    i += len(m.group(0)) - 2  # past the `(`; the loop's own step follows
                else:
                    state = 'str'
            elif c == "'":
                # A digit separator (3'000), not a character literal, when it sits
                # between two digits / hex digits; u8'x' is not used in this tree.
                if i > 0 and text[i - 1].isalnum() and i + 1 < n and text[i + 1].isalnum():
                    pass
                else:
                    state = 'chr'
        elif state == 'str' or state == 'chr':
            if c == '\\':
                i += 2
                continue
            if c == ('"' if state == 'str' else "'"):
                state = 'code'
        elif state == 'raw':
            if text.startswith(raw_end, i):
                i += len(raw_end)
                state = 'code'
                continue
        elif state == 'block':
            if text.startswith('*/', i):
                i += 2
                state = 'code'
                continue
        i += 1
    return out


def indent_of(s):
    return s[:len(s) - len(s.lstrip(' '))]


def statement_start(code_of, k):
    """Index of the line that starts the statement line k belongs to; `code_of(i)`
    is line i without its trailing comment."""
    while k > 0:
        p = code_of(k - 1).strip()
        if p == '' or p.startswith('#') or p.endswith(('{', '}', ';')) \
                or p.endswith(':') or p.endswith('*/') or p.endswith('\\'):
            return k
        k -= 1
    return k


def process(path, fix):
    text = open(path).read()
    lines = text.split('\n')
    hits = {}
    cols = dict(comment_starts(text))  # every line comment, allowed or not
    for line_no, col in cols.items():
        code = lines[line_no][:col]
        if code.strip() == '':
            continue  # whole-line comment
        tail = lines[line_no][col:].strip()
        s = code.rstrip()
        st = s.strip()
        if ALLOWED_TAIL.match(tail) and (BARE_CLOSER.match(st) or st.startswith('#') or tail.startswith('// clang-format') or 'NOLINT' in tail or 'fallthrough' in tail.lower()):
            continue
        if BARE_CLOSER.match(st) or PREPROC_KEEP.match(s) or st.endswith('\\'):
            continue
        hits[line_no] = col
    if not hits:
        return 0
    if not fix:
        for line_no in sorted(hits):
            print(f'{path}:{line_no + 1}: trailing comment: {lines[line_no].strip()[:100]}')
        return len(hits)
    # Rewrite from the bottom so earlier indices stay valid: every edit touches only
    # lines from the statement start (never above a previous, higher-up hit) down.
    out = lines[:]

    def code_of(i):
        return out[i][:cols[i]] if i in cols else out[i]
    consumed = set()
    for line_no in sorted(hits, reverse=True):
        col = hits[line_no]
        line = out[line_no]
        code = line[:col].rstrip()
        texts = [line[col + 2:].strip()]
        j = line_no + 1
        while j < len(out) and j not in consumed and out[j][:col].strip() == '' and out[j][col:col + 2] == '//' \
                and len(out[j]) > col and out[j][:col].strip() == '':
            texts.append(out[j][col + 2:].strip())
            consumed.add(j)
            j += 1
        st = code.strip()
        if st.endswith('{'):
            # inside the block, at the indentation of its first line
            nxt = j
            while nxt < len(out) and out[nxt].strip() == '':
                nxt += 1
            ind = indent_of(out[nxt]) if nxt < len(out) and not out[nxt].strip().startswith('}') else indent_of(code) + '    '
            new = [code] + [f'{ind}// {t}' if t else f'{ind}//' for t in texts]
            out[line_no:j] = new
        elif st.endswith(',') or LABEL.match(code) or st.startswith('#'):
            ind = indent_of(code)
            out[line_no:j] = [f'{ind}// {t}' if t else f'{ind}//' for t in texts] + [code]
        else:
            start = statement_start(code_of, line_no)
            ind = indent_of(out[start])
            body = out[start:line_no] + [code]
            out[start:j] = [f'{ind}// {t}' if t else f'{ind}//' for t in texts] + body
    open(path, 'w').write('\n'.join(out))
    return len(hits)


SELF_TEST = r'''#include <x>  // why
const char* url = "http://a/b";  // url
const char* lua = R"lua(
-- not a // comment
local x = '//'
)lua";  // after raw
char c = '"';  // quote char
char d = '\\';  // backslash char
int64_t big = 3'000;  // digit separator
int a = 1;  /* block */  // trailing
struct S {
    int f = 0;  // first
                // continued
    int g = 0;  // second
};
void f() {
    if (x) {  // block comment
        y();
    }
    call(a,
         b);  // wrapped
    list{
        "one",  // item one
        "two",  // item two
    };
}
}  // namespace n
#endif  // GUARD
'''

SELF_TEST_EXPECT = r'''// why
#include <x>
// url
const char* url = "http://a/b";
// after raw
const char* lua = R"lua(
-- not a // comment
local x = '//'
)lua";
// quote char
char c = '"';
// backslash char
char d = '\\';
// digit separator
int64_t big = 3'000;
// trailing
int a = 1;  /* block */
struct S {
    // first
    // continued
    int f = 0;
    // second
    int g = 0;
};
void f() {
    if (x) {
        // block comment
        y();
    }
    // wrapped
    call(a,
         b);
    list{
        // item one
        "one",
        // item two
        "two",
    };
}
}  // namespace n
#endif  // GUARD
'''


def self_test():
    import tempfile, os
    with tempfile.NamedTemporaryFile('w', suffix='.cc', delete=False) as t:
        t.write(SELF_TEST)
    try:
        process(t.name, True)
        got = open(t.name).read()
        assert got == SELF_TEST_EXPECT, '\n--- got ---\n' + got + '\n--- expected ---\n' + SELF_TEST_EXPECT
        assert process(t.name, False) == 0
    finally:
        os.unlink(t.name)
    print('check_comments: self-test ok')


def main(argv):
    if argv == ['--self-test']:
        self_test()
        return 0
    fix = '--fix' in argv
    files = [a for a in argv if a != '--fix']
    total = 0
    for f in files:
        total += process(f, fix)
    if fix:
        print(f'check_comments: moved {total} trailing comment(s)')
        return 0
    if total:
        print(f'check_comments: {total} trailing comment(s); run `make format` to move them', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
