import ast
from pathlib import Path

from event_schema import COMMON_FIELDS, EVENT_TYPES, NON_NEGATIVE_FIELDS, PAYLOAD_FIELDS

SCHEMAS_FILE = Path(__file__).resolve().parents[1] / "spark" / "schemas.py"
EXTRA_FIELDS = {"publish_time", "product_category_name"}


# Baca spark/schemas.py sebagai pohon sintaks tanpa mengimpor pyspark
def load_tree():
    return ast.parse(SCHEMAS_FILE.read_text(encoding="utf-8"))


# Ambil nilai literal dari satu konstanta di schemas.py
def read_constant(name):
    for node in load_tree().body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"konstanta {name} tidak ditemukan di {SCHEMAS_FILE}")


# Ambil semua nama kolom yang didaftarkan sebagai StructField di skema Spark
def read_spark_field_names():
    names = set()
    for node in ast.walk(load_tree()):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "StructField"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            names.add(node.args[0].value)
    return names


# Field wajib per tipe event di Spark harus sama persis dengan kontrak producer
def test_spark_required_fields_match_producer_contract():
    spark_fields = read_constant("REQUIRED_FIELDS")
    assert set(spark_fields) == set(EVENT_TYPES)
    for event_type in EVENT_TYPES:
        assert tuple(spark_fields[event_type]) == tuple(PAYLOAD_FIELDS[event_type])


# Daftar field yang tidak boleh negatif di Spark harus sama dengan producer
def test_spark_non_negative_fields_match_producer_contract():
    assert tuple(read_constant("NON_NEGATIVE_FIELDS")) == tuple(NON_NEGATIVE_FIELDS)


# Setiap field yang bisa dikirim producer harus terdaftar di skema Spark, kalau tidak Spark diam-diam membuangnya
def test_spark_schema_covers_every_event_field():
    expected = set(COMMON_FIELDS) | EXTRA_FIELDS
    for fields in PAYLOAD_FIELDS.values():
        expected |= set(fields)
    assert expected - read_spark_field_names() == set()
