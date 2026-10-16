from datetime import datetime, timedelta, timezone

import pytest

from event_schema import (
    EVENT_TYPES,
    build_event,
    is_valid,
    make_event_id,
    parse_iso,
    required_fields,
    to_iso,
    validate_event,
)

BASE_TIME = datetime(2017, 10, 2, 10, 56, 33, tzinfo=timezone.utc)

PAYLOADS = {
    "order_created": {"order_id": "o1", "customer_id": "c1", "order_status": "created"},
    "order_item": {
        "order_id": "o1",
        "order_item_id": 1,
        "product_id": "p1",
        "seller_id": "s1",
        "price": 39.99,
        "freight_value": 5.5,
        "shipping_limit_date": "2017-10-06T11:07:15.000Z",
    },
    "payment": {
        "order_id": "o1",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 2,
        "payment_value": 45.49,
    },
    "order_status_changed": {"order_id": "o1", "new_status": "approved"},
    "stock_updated": {
        "product_id": "p1",
        "warehouse_id": "WH-SP-01",
        "change_type": "restock",
        "quantity_change": 20,
        "stock_level": 120,
    },
}

PAYLOAD_FIELD_PAIRS = [
    (event_type, field) for event_type, payload in PAYLOADS.items() for field in payload
]


# Buat event valid untuk satu tipe event dari payload contoh
def make_event(event_type):
    return build_event(event_type, BASE_TIME, ("id-1",), dict(PAYLOADS[event_type]))


# Datetime naive diperlakukan sebagai UTC dan diformat dengan milidetik
def test_to_iso_treats_naive_as_utc():
    assert to_iso(datetime(2017, 10, 2, 10, 56, 33)) == "2017-10-02T10:56:33.000Z"


# Milidetik dipotong dari mikrodetik, bukan dibulatkan
def test_to_iso_keeps_milliseconds():
    value = datetime(2017, 10, 2, 10, 56, 33, 5999, tzinfo=timezone.utc)
    assert to_iso(value) == "2017-10-02T10:56:33.005Z"


# Datetime dengan zona waktu lain dikonversi ke UTC
def test_to_iso_converts_timezone_to_utc():
    value = datetime(2017, 10, 2, 17, 56, 33, tzinfo=timezone(timedelta(hours=7)))
    assert to_iso(value) == "2017-10-02T10:56:33.000Z"


# parse_iso membalik to_iso tanpa kehilangan nilai
def test_parse_iso_roundtrip():
    assert parse_iso(to_iso(BASE_TIME)) == BASE_TIME


# event_id harus sama untuk identitas yang sama, supaya duplikat bisa dideteksi
def test_make_event_id_is_deterministic():
    assert make_event_id("payment", "o1", 1) == make_event_id("payment", "o1", 1)


# event_id harus berbeda kalau tipe atau bagian identitas berbeda
def test_make_event_id_differs_by_type_and_parts():
    base = make_event_id("payment", "o1", 1)
    assert base != make_event_id("order_item", "o1", 1)
    assert base != make_event_id("payment", "o1", 2)
    assert base != make_event_id("payment", "o2", 1)


# build_event menyusun field umum dan payload dalam satu dict
def test_build_event_has_common_fields_and_payload():
    event = make_event("order_created")
    assert event["event_type"] == "order_created"
    assert event["event_time"] == "2017-10-02T10:56:33.000Z"
    assert event["order_id"] == "o1"
    assert "event_id" in event


# Daftar field wajib memuat field umum dan field payload tipe itu
def test_required_fields_include_common_and_payload():
    fields = required_fields("order_item")
    assert {"event_id", "event_type", "event_time", "price"} <= set(fields)


# Setiap tipe event yang lengkap harus lolos validasi
@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_valid_event_passes(event_type):
    event = make_event(event_type)
    assert validate_event(event) == []
    assert is_valid(event)


# Field wajib yang hilang harus dilaporkan, untuk semua tipe dan semua field
@pytest.mark.parametrize("event_type,field", PAYLOAD_FIELD_PAIRS)
def test_missing_field_is_reported(event_type, field):
    event = make_event(event_type)
    del event[field]
    assert f"field kosong: {field}" in validate_event(event)


# Nilai None dan string kosong dianggap sama dengan field hilang
@pytest.mark.parametrize("empty_value", [None, ""])
def test_empty_value_counts_as_missing(empty_value):
    event = make_event("order_created")
    event["customer_id"] = empty_value
    assert "field kosong: customer_id" in validate_event(event)


# Angka negatif pada field harga ditolak
@pytest.mark.parametrize(
    "event_type,field",
    [("order_item", "price"), ("order_item", "freight_value"), ("payment", "payment_value")],
)
def test_negative_value_is_rejected(event_type, field):
    event = make_event(event_type)
    event[field] = -1.0
    assert f"nilai negatif: {field}" in validate_event(event)


# Nilai nol bukan negatif, jadi harus lolos
def test_zero_value_is_valid():
    event = make_event("order_item")
    event["price"] = 0.0
    assert is_valid(event)


# Field harga yang bukan angka atau berupa boolean ditolak
@pytest.mark.parametrize("bad_value", ["12.5", True])
def test_non_numeric_value_is_rejected(bad_value):
    event = make_event("order_item")
    event["price"] = bad_value
    assert "field bukan angka: price" in validate_event(event)


# Tipe event yang tidak dikenal langsung ditolak dengan satu pesan
def test_unknown_event_type_is_rejected():
    event = make_event("order_created")
    event["event_type"] = "refund_requested"
    assert validate_event(event) == ["event_type tidak dikenal: refund_requested"]


# Format event_time yang rusak harus terdeteksi
def test_bad_event_time_is_rejected():
    event = make_event("order_created")
    event["event_time"] = "kemarin sore"
    assert "event_time tidak valid" in validate_event(event)
