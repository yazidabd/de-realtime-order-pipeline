# Skema event, helper timestamp, dan validasi untuk seluruh pipeline
import uuid
from datetime import datetime, timezone

EVENT_TYPES = (
    "order_created",
    "order_item",
    "payment",
    "order_status_changed",
    "stock_updated",
)

# Field wajib di semua event. publish_time ditambahkan saat event dikirim
COMMON_FIELDS = ("event_id", "event_type", "event_time")

# Field wajib per tipe event, di luar COMMON_FIELDS
PAYLOAD_FIELDS = {
    "order_created": ("order_id", "customer_id", "order_status"),
    "order_item": (
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
        "shipping_limit_date",
    ),
    "payment": (
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ),
    "order_status_changed": ("order_id", "new_status"),
    "stock_updated": (
        "product_id",
        "warehouse_id",
        "change_type",
        "quantity_change",
        "stock_level",
    ),
}

# Field numerik yang tidak boleh negatif
NON_NEGATIVE_FIELDS = ("price", "freight_value", "payment_value")

EVENT_ID_NAMESPACE = uuid.UUID("6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f")


# Ubah datetime jadi string ISO 8601 UTC dengan milidetik, contoh 2017-10-02T10:56:33.000Z
def to_iso(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


# Ubah string ISO 8601 hasil to_iso kembali jadi datetime timezone-aware
def parse_iso(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# Buat event_id deterministik dari tipe event dan bagian identitasnya, supaya event sama selalu punya id sama
def make_event_id(event_type, *parts):
    name = "|".join([event_type] + [str(part) for part in parts])
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, name))


# Susun satu event: field umum (id, tipe, waktu) digabung dengan payload spesifik tipe
def build_event(event_type, event_time, id_parts, payload):
    event = {
        "event_id": make_event_id(event_type, *id_parts),
        "event_type": event_type,
        "event_time": to_iso(event_time),
    }
    event.update(payload)
    return event


# Daftar lengkap field wajib untuk satu tipe event
def required_fields(event_type):
    return list(COMMON_FIELDS) + list(PAYLOAD_FIELDS[event_type])


# Validasi satu event, kembalikan list pesan error (list kosong berarti valid)
def validate_event(event):
    event_type = event.get("event_type")
    if event_type not in PAYLOAD_FIELDS:
        return [f"event_type tidak dikenal: {event_type}"]

    errors = []
    for field in required_fields(event_type):
        value = event.get(field)
        if value is None or value == "":
            errors.append(f"field kosong: {field}")

    for field in NON_NEGATIVE_FIELDS:
        value = event.get(field)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"field bukan angka: {field}")
        elif value < 0:
            errors.append(f"nilai negatif: {field}")

    event_time = event.get("event_time")
    if event_time not in (None, ""):
        try:
            parse_iso(event_time)
        except (TypeError, ValueError, AttributeError):
            errors.append("event_time tidak valid")

    return errors


# Shortcut: True kalau event lolos semua validasi
def is_valid(event):
    return not validate_event(event)
