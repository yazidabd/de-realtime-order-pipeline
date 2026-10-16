# Event buatan Faker: event stok dan event rusak yang sengaja disisipkan
import random

from faker import Faker

from event_schema import build_event, parse_iso, required_fields, to_iso

WAREHOUSES = ("WH-SP-01", "WH-RJ-01", "WH-MG-01", "WH-RS-01")

# Field harga yang bisa dibuat negatif
NEGATIVE_TARGETS = ("price", "payment_value")


# Buat satu nilai perubahan stok beserta jenisnya: restock, sale_adjustment, atau correction
def pick_stock_change(fake):
    roll = fake.random_int(1, 100)
    if roll <= 45:
        return "restock", fake.random_int(10, 100)
    if roll <= 90:
        return "sale_adjustment", -fake.random_int(1, 10)
    return "correction", fake.random_element([-5, -3, -2, -1, 1, 2, 3, 5])


# Buat event stock_updated dengan timestamp acak di dalam rentang waktu event Olist
def generate_stock_events(olist_events, events_per_order, seed):
    order_count = sum(1 for event in olist_events if event["event_type"] == "order_created")
    total = int(order_count * events_per_order)
    products = sorted(
        {event["product_id"] for event in olist_events if event["event_type"] == "order_item"}
    )
    if total <= 0 or not products:
        return []

    times = [parse_iso(event["event_time"]) for event in olist_events]
    start = min(times).replace(tzinfo=None)
    end = max(times).replace(tzinfo=None)

    fake = Faker()
    fake.seed_instance(seed)

    # Timestamp diurutkan dulu supaya stock_level dihitung berurutan menurut waktu
    stamps = sorted(fake.date_time_between(start_date=start, end_date=end) for _ in range(total))

    levels = {}
    events = []
    for index, stamp in enumerate(stamps):
        product_id = fake.random_element(products)
        if product_id not in levels:
            levels[product_id] = fake.random_int(20, 200)
        change_type, quantity = pick_stock_change(fake)
        levels[product_id] = max(0, levels[product_id] + quantity)
        events.append(
            build_event(
                "stock_updated",
                stamp,
                (product_id, to_iso(stamp), index),
                {
                    "product_id": product_id,
                    "warehouse_id": fake.random_element(WAREHOUSES),
                    "change_type": change_type,
                    "quantity_change": quantity,
                    "stock_level": levels[product_id],
                },
            )
        )
    return events


# Sisipkan event rusak ke stream: duplikat, field kosong, atau harga negatif, sesuai rate dari .env
def corrupt_events(events, duplicate_rate, missing_field_rate, negative_price_rate, seed):
    rng = random.Random(seed)
    stats = {"duplicate": 0, "missing_field": 0, "negative_price": 0}
    result = []

    for event in events:
        current = event

        if rng.random() < missing_field_rate:
            candidates = [f for f in required_fields(event["event_type"]) if f != "event_type"]
            field = rng.choice(candidates)
            current = dict(event)
            if rng.random() < 0.5:
                current[field] = None
            else:
                current.pop(field, None)
            stats["missing_field"] += 1
        elif rng.random() < negative_price_rate:
            targets = [f for f in NEGATIVE_TARGETS if f in event]
            if targets:
                field = rng.choice(targets)
                current = dict(event)
                current[field] = -(abs(current[field]) or 1)
                stats["negative_price"] += 1

        result.append(current)

        # Duplikat memakai event asli, jadi event_id sama dan bisa di-dedup di Spark
        if rng.random() < duplicate_rate:
            result.append(dict(event))
            stats["duplicate"] += 1

    return result, stats
