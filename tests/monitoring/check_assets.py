#!/usr/bin/env python3
"""Validates the monitoring assets under deploy/ (roadmap §5.5, docs/monitoring.md §5).

Zero C++: the gateway's metric surface is the contract, the assets consume it.
Checks, in order:
  1. deploy/prometheus/lights3.rules.yml parses; every rule is a well-formed
     recording rule or an alert with severity + summary + description; names
     are unique; `promtool check rules` when promtool is installed.
  2. deploy/prometheus/scrape.yml parses, scrapes /-/metrics and loads the rules.
  3. deploy/grafana/lights3.json is exactly what gen_dashboard.py renders, every
     panel has targets, ids are unique, the DS/instance/backend variables exist.
  4. Every lights3_* metric named by a PromQL expression (rules + dashboard)
     exists in the source catalog (grep over src/), histogram suffixes allowed.
  5. With --binary: starts the gateway on a memory backend, scrapes /-/metrics,
     checks the exposition format, and requires every referenced metric that is
     not backend-specific to be present in the live output.
Exit code 0 = all good; failures are listed on stderr.
"""
import argparse
import glob
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

METRIC_RE = re.compile(r"\blights3_[a-z0-9_]+")
# Families emitted only by a particular backend / feature: absent from a
# memory-backend gateway and therefore exempt from the live check
BACKEND_SPECIFIC = ("lights3_duostore_", "lights3_tiered_", "lights3_cloudproxy_",
                    "lights3_localfs_", "lights3_xlocalfs_", "lights3_memory_backend_",
                    "lights3_meta_cache_", "lights3_backend_pool_", "lights3_bucket_usage_",
                    "lights3_usage_", "lights3_quota_", "lights3_backend_op_seconds",
                    "lights3_backend_errors_total", "lights3_bucket_requests_total",
                    "lights3_bucket_bytes_total")
HIST_SUFFIXES = ("_bucket", "_sum", "_count")

failures = []


def fail(msg):
    failures.append(msg)
    print("FAIL:", msg, file=sys.stderr)


def ok(msg):
    print("ok:", msg)


def load_yaml(path):
    try:
        import yaml  # PyYAML
    except ImportError:
        fail(f"PyYAML not installed: cannot parse {path} (pip install pyyaml)")
        return None
    with open(path) as f:
        return yaml.safe_load(f)


def check_rules(repo):
    path = os.path.join(repo, "deploy/prometheus/lights3.rules.yml")
    doc = load_yaml(path)
    if doc is None:
        return set()
    names = set()
    exprs = []
    for g in doc.get("groups", []):
        if "name" not in g or "rules" not in g:
            fail(f"rules: group without name/rules: {g}")
            continue
        for r in g["rules"]:
            if "expr" not in r:
                fail(f"rules: {g['name']}: rule without expr: {r}")
                continue
            exprs.append(r["expr"])
            if "record" in r:
                name = r["record"]
                if not re.fullmatch(r"lights3:[a-z0-9_]+:[a-z0-9_]+", name):
                    fail(f"rules: recording rule name off convention: {name}")
            elif "alert" in r:
                name = r["alert"]
                sev = (r.get("labels") or {}).get("severity")
                if sev not in ("critical", "warning", "info"):
                    fail(f"rules: alert {name}: severity must be critical|warning|info, got {sev}")
                ann = r.get("annotations") or {}
                for k in ("summary", "description"):
                    if not ann.get(k):
                        fail(f"rules: alert {name}: missing annotation {k}")
                if not name.startswith("Lights3"):
                    fail(f"rules: alert name off convention (Lights3*): {name}")
            else:
                fail(f"rules: {g['name']}: rule is neither record nor alert: {r}")
                continue
            if name in names:
                fail(f"rules: duplicate rule name {name}")
            names.add(name)
    ok(f"rules: {len(doc.get('groups', []))} groups, {len(names)} rules well-formed")
    if shutil.which("promtool"):
        res = subprocess.run(["promtool", "check", "rules", path], capture_output=True, text=True)
        if res.returncode != 0:
            fail("promtool check rules: " + res.stdout + res.stderr)
        else:
            ok("promtool check rules")
    else:
        print("note: promtool not installed, skipping `promtool check rules`")
    return set(m for e in exprs for m in METRIC_RE.findall(e))


def check_scrape(repo):
    path = os.path.join(repo, "deploy/prometheus/scrape.yml")
    doc = load_yaml(path)
    if doc is None:
        return
    jobs = doc.get("scrape_configs") or []
    if not any(j.get("job_name") == "lights3" and j.get("metrics_path") == "/-/metrics" for j in jobs):
        fail("scrape.yml: no job lights3 with metrics_path /-/metrics")
    if "lights3.rules.yml" not in (doc.get("rule_files") or []):
        fail("scrape.yml: rule_files must load lights3.rules.yml")
    ok("scrape.yml: lights3 job + rule_files")
    if shutil.which("promtool"):
        res = subprocess.run(["promtool", "check", "config", path], capture_output=True, text=True,
                             cwd=os.path.dirname(path))
        if res.returncode != 0:
            fail("promtool check config: " + res.stdout + res.stderr)
        else:
            ok("promtool check config")


def check_dashboard(repo):
    gen_dir = os.path.join(repo, "deploy/grafana")
    sys.path.insert(0, gen_dir)
    import gen_dashboard  # noqa: E402  (renders the same JSON the file must contain)
    path = os.path.join(gen_dir, "lights3.json")
    with open(path) as f:
        text = f.read()
    if text != gen_dashboard.render():
        fail("lights3.json is stale: run python3 deploy/grafana/gen_dashboard.py")
    dash = json.loads(text)
    refs = set()
    ids = set()
    for p in dash["panels"]:
        if p["id"] in ids:
            fail(f"dashboard: duplicate panel id {p['id']}")
        ids.add(p["id"])
        if p["type"] == "row":
            continue
        if not p.get("targets"):
            fail(f"dashboard: panel without targets: {p['title']}")
        for t in p.get("targets", []):
            if not t.get("expr"):
                fail(f"dashboard: empty expr in panel {p['title']}")
            refs.update(METRIC_RE.findall(t.get("expr", "")))
    var_names = {v["name"] for v in dash["templating"]["list"]}
    for v in ("DS", "instance", "backend"):
        if v not in var_names:
            fail(f"dashboard: template variable {v} missing")
    for v in dash["templating"]["list"]:
        q = v.get("query")
        if isinstance(q, dict):
            refs.update(METRIC_RE.findall(q.get("query", "")))
    if dash.get("uid") != "lights3-overview":
        fail("dashboard: uid must stay lights3-overview (import upsert key)")
    ok(f"dashboard: {len(ids)} panels, {len(refs)} metric families referenced")
    return refs


def source_catalog(repo):
    names = set()
    for pat in ("src/**/*.cc", "src/**/*.h"):
        for path in glob.glob(os.path.join(repo, pat), recursive=True):
            with open(path, errors="replace") as f:
                names.update(METRIC_RE.findall(f.read()))
    # Histogram families are emitted as _bucket/_sum/_count
    return names


def catalog_has(catalog, name):
    if name in catalog:
        return True
    for suf in HIST_SUFFIXES:
        if name.endswith(suf) and name[: -len(suf)] in catalog:
            return True
    return False


def check_references(repo, refs):
    catalog = source_catalog(repo)
    missing = sorted(n for n in refs if not catalog_has(catalog, n))
    for n in missing:
        fail(f"metric referenced by the assets but not emitted by src/: {n}")
    if not missing:
        ok(f"all {len(refs)} referenced metric families exist in the source catalog")


def live_check(binary, refs):
    work = tempfile.mkdtemp(prefix="lights3-mon-")
    cfg = os.path.join(work, "lights3.yaml")
    log = os.path.join(work, "server.log")
    with open(cfg, "w") as f:
        f.write("http:\n  driver: builtin\n  bind: 127.0.0.1\n  port: 0\n"
                "backends:\n  - name: mem\n    type: memory\n"
                "buckets:\n  default_backend: mem\nlog:\n  level: info\n")
    with open(log, "w") as lf:
        proc = subprocess.Popen([binary, "--config", cfg], stdout=lf, stderr=subprocess.STDOUT)
    port = None
    try:
        for _ in range(100):
            with open(log, errors="replace") as lf:
                m = re.search(r"listening on 127\.0\.0\.1:(\d+)", lf.read())
            if m:
                port = m.group(1)
                break
            if proc.poll() is not None:
                break
            time.sleep(0.1)
        if not port:
            fail("live: gateway did not report its port")
            return
        base = f"http://127.0.0.1:{port}"
        # A little traffic so request-scoped families have samples
        # (the missing key yields an S3 error: lights3_s3_errors_total renders sparsely)
        for method, path in (("PUT", "/monbkt"), ("PUT", "/monbkt/k"), ("GET", "/monbkt/k"),
                             ("GET", "/monbkt/missing"), ("GET", "/-/healthz")):
            req = urllib.request.Request(base + path, method=method, data=b"x" if method == "PUT" and path.endswith("/k") else None)
            try:
                urllib.request.urlopen(req, timeout=5).read()
            except urllib.error.HTTPError:
                pass
        body = urllib.request.urlopen(base + "/-/metrics", timeout=5).read().decode()
        families = set()
        line_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})? -?[0-9.e+\-]+(?:Inf|NaN)?$|^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^}]*\})? [+-]?(?:Inf|NaN)$")
        typed = set()
        for ln in body.splitlines():
            if not ln:
                continue
            if ln.startswith("# TYPE "):
                typed.add(ln.split()[2])
                continue
            if ln.startswith("#"):
                continue
            m = line_re.match(ln)
            if not m:
                fail(f"live: line off the exposition format: {ln[:120]}")
                continue
            families.add(ln.split("{")[0].split(" ")[0])
        untyped = sorted(f for f in families
                         if f not in typed and not any(f.endswith(s) and f[: -len(s)] in typed for s in HIST_SUFFIXES))
        for f in untyped:
            fail(f"live: family without # TYPE: {f}")
        core = sorted(n for n in refs if not n.startswith(BACKEND_SPECIFIC))
        missing = [n for n in core if n not in families]
        for n in missing:
            fail(f"live: referenced core metric absent from /-/metrics on a memory-backend gateway: {n}")
        if not missing:
            ok(f"live: {len(families)} families scraped, all {len(core)} referenced core families present")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(work, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    ap.add_argument("--binary", help="lights3 binary for the live scrape check (optional)")
    args = ap.parse_args()
    refs = set()
    refs |= check_rules(args.repo)
    check_scrape(args.repo)
    refs |= check_dashboard(args.repo) or set()
    check_references(args.repo, refs)
    if args.binary:
        live_check(args.binary, refs)
    else:
        print("note: --binary not given, skipping the live scrape check")
    if failures:
        print(f"{len(failures)} failure(s)", file=sys.stderr)
        sys.exit(1)
    print("monitoring assets: all checks passed")


if __name__ == "__main__":
    main()
