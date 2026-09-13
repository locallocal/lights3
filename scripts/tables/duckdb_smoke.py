#!/usr/bin/env python3
"""DuckDB smoke against a live lights3 gateway (docs/architecture/s3-tables-design.md §13): attach the
catalog through DuckDB's iceberg extension with SigV4, list namespaces / tables, scan a
table PyIceberg wrote (run pyiceberg_smoke.py with LIGHTS3_SMOKE_KEEP=1 first, or point at any table).

    LIGHTS3_ENDPOINT=http://127.0.0.1:9000 LIGHTS3_AK=... LIGHTS3_SK=... [LIGHTS3_REGION=us-east-1]
    [LIGHTS3_TABLES_PREFIX=/iceberg] [LIGHTS3_SMOKE_BUCKET=smoke] [LIGHTS3_SMOKE_TABLE=smoke.events]
    python3 scripts/tables/duckdb_smoke.py

Needs the duckdb package; the iceberg / httpfs extensions are installed on first use
(network). Exit 0 = passed, 1 = a step failed, 3 = a dependency is missing.
"""
import os
import sys
from urllib.parse import urlparse

try:
    import duckdb
except ImportError as e:
    print(f"duckdb smoke: missing dependency: {e}", file=sys.stderr)
    sys.exit(3)

ENDPOINT = os.environ.get("LIGHTS3_ENDPOINT", "http://127.0.0.1:9000").rstrip("/")
AK = os.environ.get("LIGHTS3_AK", "")
SK = os.environ.get("LIGHTS3_SK", "")
REGION = os.environ.get("LIGHTS3_REGION", "us-east-1")
PREFIX = os.environ.get("LIGHTS3_TABLES_PREFIX", "/iceberg")
BUCKET = os.environ.get("LIGHTS3_SMOKE_BUCKET", "smoke")
TABLE = os.environ.get("LIGHTS3_SMOKE_TABLE", "smoke.events")


def main():
    if not AK or not SK:
        print("duckdb smoke: LIGHTS3_AK / LIGHTS3_SK required", file=sys.stderr)
        return 3
    u = urlparse(ENDPOINT)
    hostport = u.netloc
    use_ssl = "true" if u.scheme == "https" else "false"
    con = duckdb.connect()
    failed = 0

    def step(name, fn):
        nonlocal failed
        try:
            out = fn()
            print(f"[OK] {name}{' -- ' + str(out) if out is not None else ''}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"[FAIL] {name} -- {e}")

    step("load extensions", lambda: (con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;"), None)[1])
    step("s3 secret", lambda: (con.execute(
        f"CREATE OR REPLACE SECRET s3s (TYPE s3, PROVIDER config, KEY_ID '{AK}', SECRET '{SK}', REGION '{REGION}', "
        f"ENDPOINT '{hostport}', URL_STYLE 'path', USE_SSL {use_ssl})"), None)[1])
    step("attach catalog (sigv4)", lambda: (con.execute(
        f"ATTACH '{BUCKET}' AS c (TYPE iceberg, ENDPOINT '{ENDPOINT}{PREFIX}', AUTHORIZATION_TYPE 'sigv4', "
        f"SECRET s3s, SIGV4_REGION '{REGION}', SIGV4_SERVICE 's3')"), None)[1])
    step("list namespaces", lambda: con.execute("SELECT schema_name FROM information_schema.schemata "
                                                 "WHERE catalog_name = 'c'").fetchall())
    ns, tbl = TABLE.split(".", 1)
    step("scan table", lambda: con.execute(f'SELECT count(*) FROM c."{ns}"."{tbl}"').fetchall())
    print(f"duckdb smoke: {5 - failed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
