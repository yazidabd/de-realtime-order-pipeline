from collections import Counter
from datetime import datetime, timedelta, timezone

from event_schema import build_event, is_valid, parse_iso, validate_event
from faker_events import corrupt_events, generate_stock_events

BASE = datetime(2017, 10, 2, 10, 0, 0, tzinfo=timezone.utc)


# Buat event mirip Olist: satu order_created dan satu order_item per order
def make_olist_events(order_count):
    events = []
    for index in range(order_count):
        order_time = BASE + timedelta(minutes=index)
        order_id = f"o{index}"
        events.append(
            build_event(
                "order_created",
                order_time,
                (order_id,),
                {"order_id": order_id, "customer_id": f"c{index}", "order_status": "created"},
            )
        )
        events.append(
            build_event(
                "order_item",
                order_time + timedelta(seconds=1, milliseconds=1),
                (order_id, 1),
                {
                    "order_id": order_id,
                    "order_item_id": 1,
                    "product_id": f"p{index % 3}",
                    "seller_id": "s1",
                    "price": 10.0 + index,
                    "freight_value": 2.5,
                    "shipping_limit_date": "2017-10-06T11:07:15.000Z",
                },
            )
        )
    return events


# Jumlah event stok = jumlah order dikali rate, semuanya valid dan di dalam rentang waktu Olist
def test_stock_events_count_range_and_validity():
    olist = make_olist_events(10)
    stock = generate_stock_events(olist, 0.5, 42)
    times = [parse_iso(event["event_time"]) for event in olist]
    assert len(stock) == 5
    assert all(event["event_type"] == "stock_updated" for event in stock)
    assert all(is_valid(event) for event in stock)
    assert all(min(times) <= parse_iso(event["event_time"]) <= max(times) for event in stock)
    assert all(event["stock_level"] >= 0 for event in stock)


# Seed yang sama menghasilkan event stok yang sama persis, seed beda menghasilkan hasil beda
def test_stock_events_are_reproducible_by_seed():
    olist = make_olist_events(10)
    assert generate_stock_events(olist, 0.5, 42) == generate_stock_events(olist, 0.5, 42)
    assert generate_stock_events(olist, 0.5, 42) != generate_stock_events(olist, 0.5, 7)


# Tanpa rate atau tanpa produk, tidak ada event stok yang dibuat
def test_stock_events_empty_when_no_rate_or_no_products():
    olist = make_olist_events(10)
    only_orders = [event for event in olist if event["event_type"] == "order_created"]
    assert generate_stock_events(olist, 0, 42) == []
    assert generate_stock_events(only_orders, 0.5, 42) == []


# Rate nol berarti stream tidak berubah sama sekali
def test_corrupt_with_zero_rates_changes_nothing():
    events = make_olist_events(20)
    result, stats = corrupt_events(events, 0, 0, 0, 42)
    assert result == events
    assert stats == {"duplicate": 0, "missing_field": 0, "negative_price": 0}


# Duplikat memakai event_id yang sama dan jumlahnya sesuai rate
def test_corrupt_duplicates_share_event_id():
    events = make_olist_events(10)
    result, stats = corrupt_events(events, 1.0, 0, 0, 42)
    counts = Counter(event["event_id"] for event in result)
    assert len(result) == 2 * len(events)
    assert stats["duplicate"] == len(events)
    assert set(counts.values()) == {2}


# Event dengan field hilang selalu gagal validasi, dan event asli tidak ikut berubah
def test_corrupt_missing_field_fails_validation_without_mutating_input():
    events = make_olist_events(10)
    result, stats = corrupt_events(events, 0, 1.0, 0, 42)
    assert stats["missing_field"] == len(events)
    assert all(validate_event(event) for event in result)
    assert all(is_valid(event) for event in events)


# Harga negatif hanya menyasar event yang punya field harga
def test_corrupt_negative_price_only_hits_priced_events():
    events = make_olist_events(5)
    result, stats = corrupt_events(events, 0, 0, 1.0, 42)
    assert stats["negative_price"] == 5
    for event in result:
        if event["event_type"] == "order_item":
            assert "nilai negatif: price" in validate_event(event)
        else:
            assert is_valid(event)


# Seed yang sama menghasilkan kerusakan yang sama persis
def test_corrupt_is_reproducible_by_seed():
    events = make_olist_events(30)
    first = corrupt_events(events, 0.2, 0.2, 0.2, 42)
    second = corrupt_events(events, 0.2, 0.2, 0.2, 42)
    assert first == second
