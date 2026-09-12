#!/usr/bin/env python3
"""PyIceberg smoke against a live lights3 gateway (docs/s3-tables-design.md §13):
the RustFS script sequence -- enable the
table bucket, create namespace + table, append twice, reload + scan, a stale-handle commit
conflict, an idempotent replay, maintenance plan / run, drop with purge.

    LIGHTS3_ENDPOINT=http://127.0.0.1:9000 LIGHTS3_AK=... LIGHTS3_SK=... [LIGHTS3_REGION=us-east-1]
    [LIGHTS3_TABLES_PREFIX=/iceberg] [LIGHTS3_SMOKE_BUCKET=smoke] python3 scripts/tables/pyiceberg_smoke.py

Needs pyiceberg (with pyarrow), boto3 (PyIceberg's SigV4) and botocore (the admin calls here).
Exit 0 = every step passed, 1 = a step failed, 3 = a dependency is missing.
"""
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

try:
    import pyarrow as pa
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType
except ImportError as e:
    print(f"pyiceberg smoke: missing dependency: {e}", file=sys.stderr)
    sys.exit(3)

ENDPOINT = os.environ.get("LIGHTS3_ENDPOINT", "http://127.0.0.1:9000").rstrip("/")
AK = os.environ.get("LIGHTS3_AK", "")
SK = os.environ.get("LIGHTS3_SK", "")
REGION = os.environ.get("LIGHTS3_REGION", "us-east-1")
PREFIX = os.environ.get("LIGHTS3_TABLES_PREFIX", "/iceberg")
BUCKET = os.environ.get("LIGHTS3_SMOKE_BUCKET", "smoke")
NS = "smoke"
TABLE = f"{NS}.events"

steps = []


def step(name, ok, detail=""):
    steps.append((name, ok))
    print(f"[{'OK' if ok else 'FAIL'}] {name}{(' -- ' + detail) if detail else ''}")
    if not ok:
        raise SystemExit(finish())


def finish():
    failed = sum(1 for _, ok in steps if not ok)
    print(f"pyiceberg smoke: {len(steps) - failed} passed, {failed} failed")
    return 1 if failed else 0


def signed(method, path, body=b"", service="s3", headers=None):
    """SigV4-signed request against the gateway (catalog + S3 + admin planes)."""
    url = ENDPOINT + path
    # the S3 / admin planes require the payload hash header (lights3-ctl sends it too);
    # the catalog plane accepts generic SigV4 without it
    hdrs = dict(headers or {})
    hdrs.setdefault("x-amz-content-sha256", hashlib.sha256(body).hexdigest())
    req = AWSRequest(method=method, url=url, data=body, headers=hdrs)
    SigV4Auth(Credentials(AK, SK), service, REGION).add_auth(req)
    prepared = req.prepare()
    http = urllib.request.Request(url, data=body if body else None, method=method, headers=dict(prepared.headers))
    try:
        with urllib.request.urlopen(http, timeout=30) as r:
            return r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers)


def wait_job(ident, job):
    """Poll GET .../maintenance/jobs/<id> until the job is finished; returns its document."""
    doc = {}
    for _ in range(300):
        _, body, _ = signed("GET", f"{ident}/maintenance/jobs/{job}")
        doc = json.loads(body)
        if not doc.get("running"):
            break
        time.sleep(0.1)
    return doc


def main():
    if not AK or not SK:
        print("pyiceberg smoke: LIGHTS3_AK / LIGHTS3_SK required", file=sys.stderr)
        return 3
    # ---- bucket + table bucket (root) ----
    st, _, _ = signed("PUT", f"/{BUCKET}")
    step("create bucket", st in (200, 409), f"status {st}")
    st, body, _ = signed("PUT", f"{PREFIX}/v1/buckets/{BUCKET}", b"{}", headers={"Content-Type": "application/json"})
    step("enable table bucket", st == 200, body.decode()[:200])

    # PyIceberg's SigV4 signer takes the credentials from the boto3 default chain
    os.environ.setdefault("AWS_ACCESS_KEY_ID", AK)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", SK)
    os.environ.setdefault("AWS_DEFAULT_REGION", REGION)
    catalog = load_catalog(
        "lights3",
        **{
            "type": "rest",
            "uri": ENDPOINT + PREFIX,
            "warehouse": BUCKET,
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3",
            "rest.signing-region": REGION,
            "s3.endpoint": ENDPOINT,
            "s3.access-key-id": AK,
            "s3.secret-access-key": SK,
            "s3.region": REGION,
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
    # ---- namespace + table (a leftover table from an earlier run is purged first) ----
    ident = f"{PREFIX}/v1/{BUCKET}/namespaces/{NS}/tables/events"
    if (NS,) in catalog.list_namespaces():
        st, _, hdr = signed("DELETE", f"{ident}?purgeRequested=true")
        job_hdr = {k.lower(): v for k, v in hdr.items()}.get("x-lights3-job-id")
        if st == 204 and job_hdr:
            wait_job(ident, int(job_hdr))
    if (NS,) not in catalog.list_namespaces():
        catalog.create_namespace(NS)
    step("namespace", (NS,) in catalog.list_namespaces())
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )
    table = catalog.create_table(TABLE, schema=schema)
    step("create table", table.metadata.table_uuid is not None, table.metadata_location)
    # ---- append twice, reload, scan ----
    rows = pa.Table.from_pylist([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], schema=schema.as_arrow())
    table.append(rows)
    table.append(pa.Table.from_pylist([{"id": 3, "name": "c"}], schema=schema.as_arrow()))
    fresh = catalog.load_table(TABLE)
    scanned = fresh.scan().to_arrow()
    step("append twice + reload + scan", scanned.num_rows == 3, f"{scanned.num_rows} rows, "
         f"{len(fresh.metadata.snapshots)} snapshots, gen {fresh.metadata.metadata_log and len(fresh.metadata.metadata_log)}")
    # ---- commit conflict on a stale handle ----
    stale = catalog.load_table(TABLE)
    fresh.append(pa.Table.from_pylist([{"id": 4, "name": "d"}], schema=schema.as_arrow()))
    conflicted = False
    try:
        stale.append(pa.Table.from_pylist([{"id": 5, "name": "e"}], schema=schema.as_arrow()))
    except CommitFailedException:
        conflicted = True
    # PyIceberg may refresh and retry on its own; either a conflict or a successful
    # serialization after refresh is acceptable, the table must stay consistent
    latest = catalog.load_table(TABLE)
    n = latest.scan().to_arrow().num_rows
    step("stale-handle commit", n in (4, 5) and (conflicted or n == 5), f"conflict={conflicted}, {n} rows")
    # ---- idempotent replay: the same commit-id twice through the REST endpoint ----
    st, body, _ = signed("GET", ident)
    step("LoadTable (signed)", st == 200)
    md = json.loads(body)
    current = md["metadata"]["current-snapshot-id"]
    commit = {
        "commit-id": "smoke-000000-0000-4000-8000-000000000001",
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current}],
        "updates": [{"action": "set-properties", "updates": {"smoke": "1"}}],
    }
    st1, b1, _ = signed("POST", ident, json.dumps(commit).encode(), headers={"Content-Type": "application/json"})
    st2, b2, _ = signed("POST", ident, json.dumps(commit).encode(), headers={"Content-Type": "application/json"})
    g1 = json.loads(b1).get("generation")
    g2 = json.loads(b2).get("generation")
    step("idempotent replay", st1 == 200 and st2 == 200 and g1 == g2, f"generation {g1} / {g2}")
    # ---- maintenance plan / run (admin plane, root) ----
    st, body, _ = signed("POST", f"/-/admin/tables/{BUCKET}/{NS}/events/plan")
    step("maintenance plan started", st == 202, body.decode()[:120])
    job = json.loads(body)["job_id"]
    doc = wait_job(ident, job)
    step("maintenance plan finished", "stats" in doc and "version-token" in doc["stats"],
         f"{len(doc.get('stats', {}).get('metadata-candidates', []))} metadata candidates, "
         f"{len(doc.get('stats', {}).get('orphan-candidates', []))} orphan candidates")
    st, body, _ = signed("POST", f"/-/admin/tables/{BUCKET}/{NS}/events/run", json.dumps({"job_id": job}).encode(),
                         headers={"Content-Type": "application/json"})
    step("maintenance run started", st == 202, body.decode()[:120])
    job = json.loads(body)["job_id"]
    doc = wait_job(ident, job)
    step("maintenance run finished", "stats" in doc and "error" not in doc, json.dumps(doc.get("stats")))
    # ---- diagnostics: everything committed ----
    st, body, _ = signed("GET", f"{ident}/catalog/diagnostics")
    diag = json.loads(body)
    states = {c["state"] for c in diag.get("commits", [])}
    step("diagnostics", st == 200 and states <= {"Committed"}, f"states {sorted(states)}")
    # ---- drop with purge (LIGHTS3_SMOKE_KEEP=1 leaves the table for duckdb_smoke.py) ----
    if os.environ.get("LIGHTS3_SMOKE_KEEP") == "1":
        print("[OK] keep table (LIGHTS3_SMOKE_KEEP=1)")
        return finish()
    st, _, hdr = signed("DELETE", f"{ident}?purgeRequested=true")
    job_hdr = {k.lower(): v for k, v in hdr.items()}.get("x-lights3-job-id")
    step("drop with purge", st == 204 and job_hdr is not None, f"status {st}")
    doc = wait_job(ident, int(job_hdr))
    step("purge job", doc.get("stats", {}).get("tombstone_removed") is True, json.dumps(doc.get("stats")))
    gone = False
    try:
        catalog.load_table(TABLE)
    except NoSuchTableError:
        gone = True
    step("table gone", gone)
    return finish()


if __name__ == "__main__":
    sys.exit(main())
