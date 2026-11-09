#!/usr/bin/env python3
"""Generates presto-lance/src/test/resources/vector_db, a tiny Lance dataset with a
FixedSizeList vector column for vector_search TVF tests. Deterministic: row i has
vec = [i, 0, 0, 0], so L2 distance to query [0.3, 0, 0, 0] is (i - 0.3)^2 — strictly
increasing in i, making top-k assertions exact. No vector index: the scan uses Lance's
flat (exact) search path, which is also what the fragments-not-covered-by-index split takes.

Run once from the repo root:  python3 presto-lance/src/test/resources/generate_vector_db.py
Requires: pip3 install pylance pyarrow  (or: uv run --with pylance --with pyarrow ...)
"""
import os
import shutil

import lance
import pyarrow as pa

out_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vector_db")
dataset_path = os.path.join(out_root, "vectors.lance")
if os.path.exists(dataset_path):
    shutil.rmtree(dataset_path)
os.makedirs(out_root, exist_ok=True)

DIM = 4
N = 20
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("vec", pa.list_(pa.float32(), DIM), nullable=False),
    pa.field("tag", pa.string(), nullable=True),
])
table = pa.table({
    "id": pa.array(range(N), pa.int64()),
    "vec": pa.array([[float(i), 0.0, 0.0, 0.0] for i in range(N)], pa.list_(pa.float32(), DIM)),
    "tag": pa.array(["even" if i % 2 == 0 else "odd" for i in range(N)], pa.string()),
}, schema=schema)

# Two fragments so multi-fragment merge paths are exercised.
# data_storage_version pinned: the connector's Java SDK is lance-core 2.0.1 while local
# pylance is 4.x, whose default file format may be newer than the Java reader supports.
lance.write_dataset(table.slice(0, 10), dataset_path, schema=schema, data_storage_version="2.0", enable_v2_manifest_paths=False)
lance.write_dataset(table.slice(10, 10), dataset_path, schema=schema, mode="append", data_storage_version="2.0", enable_v2_manifest_paths=False)
print("wrote", dataset_path, "version", lance.dataset(dataset_path).version)
