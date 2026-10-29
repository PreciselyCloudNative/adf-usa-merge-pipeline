"""Column contract for adf_usa (source: precisely_product_share.precisely_db_property_graph.adf_usa).

Order matters: HASH_COLUMNS defines the canonical order used by the row hash.
Changing it, or the column set, is a hash-spec change (bump HASH_SPEC_VERSION).
"""

KEY_COLUMN = "PBKEY"

# (name, type) for every non-key column, in source order. Hashed in this order.
HASH_COLUMNS: list[tuple[str, str]] = [
    ("PARENT", "string"),
    ("GOVID", "string"),
    ("GEOID", "string"),
    ("FORMATTEDADDRESS", "string"),
    ("ADDRLINE1", "string"),
    ("ADDRLINE2", "string"),
    ("ADDRLINE3", "string"),
    ("ADDRLINE4", "string"),
    ("ADDNUMBER", "string"),
    ("ADDNUMBER2", "string"),
    ("STREETNAME", "string"),
    ("STREETPREDIR", "string"),
    ("STREET", "string"),
    ("STREETSUFFIX", "string"),
    ("STREETPOSTDIR", "string"),
    ("UNITTYPE", "string"),
    ("UNIT", "string"),
    ("LEVEL", "string"),
    ("BUILDINGNAME", "string"),
    ("CITY", "string"),
    ("POSTALCODE", "string"),
    ("POSTALCODEEXT", "string"),
    ("ADMIN2", "string"),
    ("ADMIN1", "string"),
    ("COUNTRY", "string"),
    ("LOCCODE", "string"),
    ("LATITUDE", "double"),
    ("LONGITUDE", "double"),
    ("PROPTYPE", "string"),
    ("LANGCODE", "string"),
    ("FIPS", "string"),
]

SOURCE_COLUMNS: list[str] = [KEY_COLUMN] + [name for name, _ in HASH_COLUMNS]

assert len(SOURCE_COLUMNS) == 32
