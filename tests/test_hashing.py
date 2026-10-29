"""Reference-hash tests (no Spark needed)."""

import hashlib
import struct

from adf_merge.hashing import HASH_BYTES, HEADER, row_hash_py
from adf_merge.schema import HASH_COLUMNS


def _row(**overrides):
    row = {name: None for name, _ in HASH_COLUMNS}
    row.update(overrides)
    return row


def test_hash_length():
    assert len(row_hash_py(_row())) == HASH_BYTES


def test_null_differs_from_empty_string():
    assert row_hash_py(_row(UNIT=None)) != row_hash_py(_row(UNIT=""))


def test_any_column_change_changes_hash():
    base = row_hash_py(_row(CITY="AUSTIN", LATITUDE=30.2672, LONGITUDE=-97.7431))
    for name, col_type in HASH_COLUMNS:
        value = 1.0 if col_type == "double" else "X"
        changed = _row(CITY="AUSTIN", LATITUDE=30.2672, LONGITUDE=-97.7431)
        changed[name] = value if changed[name] != value else (2.0 if col_type == "double" else "Y")
        assert row_hash_py(changed) != base, name


def test_adjacent_column_values_do_not_collide():
    # Length-prefixing keeps ("AB","C") and ("A","BC") apart.
    a = row_hash_py(_row(STREETNAME="AB", STREET="C"))
    b = row_hash_py(_row(STREETNAME="A", STREET="BC"))
    assert a != b


def test_latitude_is_exact_not_rounded():
    a = row_hash_py(_row(LATITUDE=30.267200000000001))
    b = row_hash_py(_row(LATITUDE=30.26720000000001))  # differs in the 14th decimal
    assert a != b


def test_negative_zero_is_distinct_from_zero():
    assert row_hash_py(_row(LATITUDE=-0.0)) != row_hash_py(_row(LATITUDE=0.0))


def test_utf8_multibyte_length_is_byte_length():
    assert row_hash_py(_row(CITY="Zürich")) != row_hash_py(_row(CITY="Zurich"))


def test_golden_vector_all_null_row():
    expected_encoding = HEADER + b"\x00" * len(HASH_COLUMNS)
    expected = hashlib.sha256(expected_encoding).digest()[:HASH_BYTES]
    assert row_hash_py(_row()) == expected
    # Pinned value: changing this means the hash spec changed -> bump HASH_SPEC_VERSION.
    assert row_hash_py(_row()).hex() == hashlib.sha256(HEADER + b"\x00" * 31).hexdigest()[:32]


def test_golden_vector_populated_row():
    row = _row(CITY="AUSTIN", ADMIN1="TX", POSTALCODE="78701", LATITUDE=30.2672, LONGITUDE=-97.7431)
    enc = bytearray(HEADER)
    for name, col_type in HASH_COLUMNS:
        v = row[name]
        if v is None:
            enc += b"\x00"
        elif col_type == "string":
            raw = v.encode()
            enc += b"\x01" + struct.pack(">I", len(raw)) + raw
        else:
            enc += b"\x01" + struct.pack(">d", v)
    assert row_hash_py(row) == hashlib.sha256(bytes(enc)).digest()[:HASH_BYTES]
