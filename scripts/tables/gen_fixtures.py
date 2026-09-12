#!/usr/bin/env python3
"""Generate the Iceberg manifest-list / manifest Avro fixtures under tests/fixtures/tables
(docs/s3-tables/step-3-validation-diagnostics.md §10) with PyIceberg's own writers, and
dump what PyIceberg reads back as <name>.json so the C++ reader can be checked against it.

    PYTHONPATH=<dir with pyiceberg+pyarrow> python3 scripts/tables/gen_fixtures.py

Paths inside the fixtures are fixed (bucket tbk, table n/t; bucket tbe2e for the e2e
segment): the unit tests put objects of the recorded sizes at exactly these keys.
"""
import json
import os
import sys
import tempfile

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    read_manifest_list,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tests", "fixtures", "tables")
SCHEMA = Schema(NestedField(1, "id", LongType(), required=True), schema_id=0)
ADDED, EXISTING, DELETED = ManifestEntryStatus.ADDED, ManifestEntryStatus.EXISTING, ManifestEntryStatus.DELETED

F1 = "s3://tbk/n/t/data/f1.parquet"
F2 = "s3://tbk/n/t/data/f2.parquet"
F3 = "s3://tbk/n/t/data/f3.parquet"
GHOST = "s3://tbk/n/t/data/ghost.parquet"

# name, snapshot id, parent, sequence number, entries (status, path, size, rows, snapshot id the entry is
# attributed to: the adding snapshot for ADDED / EXISTING, the deleting one for DELETED)
CASES = [
    ("1", 1, None, 1, [(ADDED, F1, 100, 10, 1), (ADDED, F2, 200, 20, 1)]),
    ("2-readd", 2, 1, 2, [(EXISTING, F2, 200, 20, 1), (ADDED, F1, 100, 10, 2)]),
    ("3-delete", 3, 1, 2, [(DELETED, F1, 100, 10, 3), (EXISTING, F2, 200, 20, 1)]),
    ("4-ghost", 4, 1, 2, [(DELETED, GHOST, 5, 1, 4), (EXISTING, F1, 100, 10, 1), (EXISTING, F2, 200, 20, 1)]),
    ("5-append", 5, 1, 2, [(EXISTING, F1, 100, 10, 1), (EXISTING, F2, 200, 20, 1), (ADDED, F3, 300, 30, 5)]),
    ("foreign", 6, None, 1, [(ADDED, "s3://other/x.parquet", 7, 1, 6)]),
    ("e2e", 1, None, 1, [(ADDED, "s3://tbe2e/e2e/demo/orders/data/f1.parquet", 13, 1, 1)]),
]


def data_file(path, size, rows):
    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=rows,
        file_size_in_bytes=size,
        column_sizes={1: size // 2},
        value_counts={1: rows},
        null_value_counts={1: 0},
        nan_value_counts={},
        lower_bounds={1: b"\x01\x00\x00\x00\x00\x00\x00\x00"},
        upper_bounds={1: b"\x09\x00\x00\x00\x00\x00\x00\x00"},
        key_metadata=None,
        split_offsets=[4],
        equality_ids=None,
        sort_order_id=None,
    )


def write_case(name, snapshot_id, parent, seq, entries, codec, tmp):
    suffix = "" if codec == "null" else "-" + codec
    bucket = "tbe2e" if name == "e2e" else "tbk"
    table = "e2e/demo/orders" if name == "e2e" else "n/t"
    m_name = f"m-{name}{suffix}.avro"
    ml_name = f"ml-{name}{suffix}.avro"
    io = PyArrowFileIO()
    m_local = os.path.join(tmp, m_name)
    w = write_manifest(2, UNPARTITIONED_PARTITION_SPEC, SCHEMA, io.new_output("file://" + m_local), snapshot_id, codec)
    with w as mw:
        for status, path, size, rows, snap in entries:
            mw.add_entry(
                ManifestEntry.from_args(
                    status=status,
                    snapshot_id=snap,
                    sequence_number=None if status == ADDED else seq - 1,
                    file_sequence_number=None if status == ADDED else seq - 1,
                    data_file=data_file(path, size, rows),
                )
            )
    mf = w.to_manifest_file()
    m_bytes = open(m_local, "rb").read()
    # the manifest-list must carry the path the catalog will see, not the local one
    mf2 = ManifestFile.from_args(
        manifest_path=f"s3://{bucket}/{table}/metadata/{m_name}",
        manifest_length=len(m_bytes),
        partition_spec_id=mf.partition_spec_id,
        content=mf.content,
        sequence_number=mf.sequence_number,
        min_sequence_number=mf.min_sequence_number,
        added_snapshot_id=mf.added_snapshot_id,
        added_files_count=mf.added_files_count,
        existing_files_count=mf.existing_files_count,
        deleted_files_count=mf.deleted_files_count,
        added_rows_count=mf.added_rows_count,
        existing_rows_count=mf.existing_rows_count,
        deleted_rows_count=mf.deleted_rows_count,
        partitions=mf.partitions,
        key_metadata=None,
    )
    ml_local = os.path.join(tmp, ml_name)
    with write_manifest_list(2, io.new_output("file://" + ml_local), snapshot_id, parent, seq, codec) as lw:
        lw.add_manifests([mf2])
    ml_bytes = open(ml_local, "rb").read()
    open(os.path.join(OUT, m_name), "wb").write(m_bytes)
    open(os.path.join(OUT, ml_name), "wb").write(ml_bytes)
    # what PyIceberg reads back (the reference the C++ reader is compared with)
    manifests = read_manifest_list(io.new_input("file://" + ml_local))
    dump = {"manifest-list": [], "manifest": []}
    for m in manifests:
        dump["manifest-list"].append(
            {
                "manifest_path": m.manifest_path,
                "manifest_length": m.manifest_length,
                "partition_spec_id": m.partition_spec_id,
                "content": int(m.content),
                "sequence_number": m.sequence_number,
                "min_sequence_number": m.min_sequence_number,
                "added_snapshot_id": m.added_snapshot_id,
                "added_files_count": m.added_files_count,
                "existing_files_count": m.existing_files_count,
                "deleted_files_count": m.deleted_files_count,
            }
        )
        local_m = ManifestFile.from_args(
            manifest_path="file://" + m_local,
            manifest_length=m.manifest_length,
            partition_spec_id=m.partition_spec_id,
            content=m.content,
            sequence_number=m.sequence_number,
            min_sequence_number=m.min_sequence_number,
            added_snapshot_id=m.added_snapshot_id,
        )
        for e in local_m.fetch_manifest_entry(io, discard_deleted=False):
            dump["manifest"].append(
                {
                    "status": int(e.status),
                    "snapshot_id": e.snapshot_id,
                    "sequence_number": e.sequence_number,
                    "file_sequence_number": e.file_sequence_number,
                    "content": int(e.data_file.content),
                    "file_path": e.data_file.file_path,
                    "file_format": e.data_file.file_format.value,
                    "record_count": e.data_file.record_count,
                    "file_size_in_bytes": e.data_file.file_size_in_bytes,
                    "split_offsets": e.data_file.split_offsets,
                }
            )
    json.dump(dump, open(os.path.join(OUT, f"{name}{suffix}.json"), "w"), indent=1, sort_keys=True)
    print(f"{ml_name} {len(ml_bytes)} B, {m_name} {len(m_bytes)} B")


def main():
    os.makedirs(OUT, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        for name, sid, parent, seq, entries in CASES:
            codecs = ["null", "deflate"] if name == "1" else ["null"]
            for codec in codecs:
                write_case(name, sid, parent, seq, entries, codec, tmp)


if __name__ == "__main__":
    sys.exit(main())
