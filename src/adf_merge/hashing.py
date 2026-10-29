"""Row hash for adf_usa (hash spec v1).

Canonical encoding, per row, over HASH_COLUMNS in order (PBKEY excluded):
    header   b"ADF1"
    string   NULL -> 0x00 ; else 0x01 + uint32 big-endian UTF-8 byte length + UTF-8 bytes
    double   NULL -> 0x00 ; else 0x01 + IEEE-754 binary64, big-endian (exact value, no rounding)
Hash = first 16 bytes of SHA-256 over the encoding.

NULL and the empty string are distinct. Non-finite doubles are rejected by the release gate,
so NaN payload canonicalisation is not part of the spec.

`row_hash_py` is the reference implementation (tests, golden vectors). `row_hash_expr` is the
Spark implementation and must match it byte for byte (tests/test_hashing_spark.py).
"""

import hashlib
import struct

from adf_merge.schema import HASH_COLUMNS

HASH_SPEC_VERSION = 1
HEADER = b"ADF1"
HASH_BYTES = 16


def _encode_py(value, col_type: str) -> bytes:
    if value is None:
        return b"\x00"
    if col_type == "string":
        raw = value.encode("utf-8")
        return b"\x01" + struct.pack(">I", len(raw)) + raw
    if col_type == "double":
        return b"\x01" + struct.pack(">d", value)
    raise ValueError(f"unsupported column type {col_type!r}")


def row_hash_py(row: dict) -> bytes:
    """Reference hash of one row (a mapping of column name -> python value)."""
    parts = [HEADER]
    for name, col_type in HASH_COLUMNS:
        parts.append(_encode_py(row.get(name), col_type))
    return hashlib.sha256(b"".join(parts)).digest()[:HASH_BYTES]


def row_hash_expr():
    """Spark Column computing the same hash. Imported lazily so the reference needs no Spark."""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("binary")
    def _double_bytes(s: pd.Series) -> pd.Series:
        # 0x00 for NULL, else 0x01 + big-endian IEEE-754 bits (identical to struct.pack(">d")).
        return s.map(lambda v: b"\x00" if pd.isna(v) else b"\x01" + struct.pack(">d", v))

    def _string_bytes(name: str):
        c = F.col(name)
        raw = F.encode(c, "UTF-8")
        length = F.unhex(F.lpad(F.hex(F.length(raw)), 8, "0"))
        return F.when(c.isNull(), F.lit(b"\x00")).otherwise(F.concat(F.lit(b"\x01"), length, raw))

    parts = [F.lit(HEADER)]
    for name, col_type in HASH_COLUMNS:
        parts.append(_string_bytes(name) if col_type == "string" else _double_bytes(F.col(name)))
    digest_hex = F.sha2(F.concat(*parts), 256)
    return F.unhex(F.substring(digest_hex, 1, HASH_BYTES * 2))
