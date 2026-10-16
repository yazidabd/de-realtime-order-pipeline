import pytest

from event_schema import is_valid
from olist_loader import load_olist_events

ORDERS_CSV = """order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date
o1,c1,delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00
o2,c2,canceled,2017-10-03 09:00:00,,,,2017-10-20 00:00:00
o3,c3,created,2017-10-05 08:00:00,,,,2017-10-25 00:00:00
"""

ITEMS_CSV = """order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value
o1,1,p1,s1,2017-10-06 11:07:15,29.99,8.72
o1,2,p2,s2,2017-10-06 11:07:15,10.00,2.50
o2,1,p1,s1,2017-10-09 09:00:00,29.99,8.72
o3,1,p2,s2,2017-10-10 08:00:00,10.00,2.50
"""

PAYMENTS_CSV = """order_id,payment_sequential,payment_type,payment_installments,payment_value
o1,1,credit_card,2,40.00
o1,2,voucher,1,10.00
o2,1,boleto,1,20.50
o3,1,credit_card,1,10.00
"""

PRODUCTS_CSV = """product_id,product_category_name,product_weight_g
p1,moveis_decoracao,500
p2,,300
"""


# Tulis empat CSV mini ala Olist ke folder sementara dan kembalikan path-nya
@pytest.fixture
def olist_dir(tmp_path):
    files = {
        "olist_orders_dataset.csv": ORDERS_CSV,
        "olist_order_items_dataset.csv": ITEMS_CSV,
        "olist_order_payments_dataset.csv": PAYMENTS_CSV,
        "olist_products_dataset.csv": PRODUCTS_CSV,
    }
    for name, content in files.items():
        (tmp_path / name).write_text(content)
    return tmp_path


# Ambil semua event dengan tipe tertentu
def of_type(events, event_type):
    return [event for event in events if event["event_type"] == event_type]


# Ambil satu event yang cocok dengan tipe dan filter field, gagal kalau jumlahnya bukan satu
def find_one(events, event_type, **match):
    found = [
        event
        for event in of_type(events, event_type)
        if all(event.get(key) == value for key, value in match.items())
    ]
    assert len(found) == 1
    return found[0]


# Jumlah event per tipe sesuai data mini, dan semuanya lolos validasi
def test_event_counts_and_validity(olist_dir):
    events = load_olist_events(olist_dir)
    assert len(of_type(events, "order_created")) == 3
    assert len(of_type(events, "order_item")) == 4
    assert len(of_type(events, "payment")) == 4
    assert len(of_type(events, "order_status_changed")) == 3
    assert all(is_valid(event) for event in events)


# order_created memakai purchase timestamp apa adanya (offset 0 detik)
def test_order_created_uses_purchase_timestamp(olist_dir):
    events = load_olist_events(olist_dir)
    created = find_one(events, "order_created", order_id="o1")
    assert created["event_time"] == "2017-10-02T10:56:33.000Z"


# order_item = purchase + 1 detik + order_item_id dalam milidetik
def test_item_offset_is_one_second_plus_item_id_millis(olist_dir):
    events = load_olist_events(olist_dir)
    assert find_one(events, "order_item", order_id="o1", order_item_id=1)["event_time"] == "2017-10-02T10:56:34.001Z"
    assert find_one(events, "order_item", order_id="o1", order_item_id=2)["event_time"] == "2017-10-02T10:56:34.002Z"


# payment = purchase + 2 detik + payment_sequential dalam milidetik
def test_payment_offset_is_two_seconds_plus_sequential_millis(olist_dir):
    events = load_olist_events(olist_dir)
    assert find_one(events, "payment", order_id="o1", payment_sequential=1)["event_time"] == "2017-10-02T10:56:35.001Z"
    assert find_one(events, "payment", order_id="o1", payment_sequential=2)["event_time"] == "2017-10-02T10:56:35.002Z"


# Status changed diturunkan dari tiga kolom, kolom null dilewati, estimated delivery bukan event
def test_status_events_skip_null_columns_and_ignore_estimated_date(olist_dir):
    events = load_olist_events(olist_dir)
    status_events = of_type(events, "order_status_changed")
    by_status = {event["new_status"]: event["event_time"] for event in status_events if event["order_id"] == "o1"}
    assert by_status == {
        "approved": "2017-10-02T11:07:15.000Z",
        "shipped": "2017-10-04T19:55:00.000Z",
        "delivered": "2017-10-10T21:25:13.000Z",
    }
    assert not [event for event in status_events if event["order_id"] != "o1"]
    assert not [event for event in events if event["event_time"].startswith("2017-10-18")]


# shipping_limit_date hanya atribut item, bukan event_time
def test_shipping_limit_date_is_attribute_not_event_time(olist_dir):
    events = load_olist_events(olist_dir)
    item = find_one(events, "order_item", order_id="o1", order_item_id=1)
    assert item["shipping_limit_date"] == "2017-10-06T11:07:15.000Z"
    assert item["event_time"] != item["shipping_limit_date"]


# Kategori produk ikut ter-join, kategori kosong menjadi None
def test_product_category_is_joined_and_blank_becomes_none(olist_dir):
    events = load_olist_events(olist_dir)
    assert find_one(events, "order_item", order_id="o1", order_item_id=1)["product_category_name"] == "moveis_decoracao"
    assert find_one(events, "order_item", order_id="o1", order_item_id=2)["product_category_name"] is None


# max_orders mengambil order paling awal menurut waktu beli, dan item payment ikut terfilter
def test_max_orders_takes_earliest_orders(olist_dir):
    events = load_olist_events(olist_dir, max_orders=2)
    assert {event["order_id"] for event in of_type(events, "order_created")} == {"o1", "o2"}
    assert len(of_type(events, "order_item")) == 3
    assert len(of_type(events, "payment")) == 3


# Dua kali load menghasilkan event_id yang identik (dasar dedup dan ledger)
def test_event_ids_are_stable_across_loads(olist_dir):
    first = {event["event_id"] for event in load_olist_events(olist_dir)}
    second = {event["event_id"] for event in load_olist_events(olist_dir)}
    assert first == second
    assert len(first) == 14


# File Olist yang hilang menghasilkan error yang jelas
def test_missing_file_raises_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_olist_events(tmp_path)


# skip_orders melewati order paling awal, jadi event yang dihasilkan tidak tumpang tindih dengan batch sebelumnya
def test_skip_orders_yields_disjoint_events(olist_dir):
    first = load_olist_events(olist_dir, max_orders=1)
    second = load_olist_events(olist_dir, max_orders=1, skip_orders=1)
    first_ids = {event["event_id"] for event in first}
    second_ids = {event["event_id"] for event in second}
    assert first_ids.isdisjoint(second_ids)
    assert {event["order_id"] for event in of_type(second, "order_created")} == {"o2"}
