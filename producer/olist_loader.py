# Loader dataset Olist: baca CSV, join lewat order_id, ubah jadi event dengan timestamp deterministik
from datetime import timedelta
from pathlib import Path

import pandas as pd

from event_schema import build_event, to_iso

ORDERS_FILE = "olist_orders_dataset.csv"
ITEMS_FILE = "olist_order_items_dataset.csv"
PAYMENTS_FILE = "olist_order_payments_dataset.csv"
PRODUCTS_FILE = "olist_products_dataset.csv"

ORDER_TIME_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
]

# Pasangan kolom timestamp dan nama status untuk event order_status_changed
STATUS_COLUMNS = (
    ("order_approved_at", "approved"),
    ("order_delivered_carrier_date", "shipped"),
    ("order_delivered_customer_date", "delivered"),
)


# Pastikan file CSV ada, kalau tidak beri pesan error yang jelas
def require_file(path):
    if not path.exists():
        raise FileNotFoundError(f"File Olist tidak ditemukan: {path}")
    return path


# Ubah Timestamp pandas jadi datetime Python, atau None kalau nilainya kosong
def to_datetime_or_none(value):
    if pd.isna(value):
        return None
    return value.to_pydatetime()


# Ubah nilai teks pandas jadi str, atau None kalau nilainya kosong
def clean_text(value):
    if pd.isna(value):
        return None
    return str(value)


# Baca tabel orders, urutkan berdasarkan waktu beli, lewati skip_orders pertama, lalu ambil max_orders (0 berarti semua)
def read_orders(data_dir, max_orders, skip_orders=0):
    path = require_file(data_dir / ORDERS_FILE)
    orders = pd.read_csv(path, parse_dates=ORDER_TIME_COLUMNS)
    orders = orders.sort_values("order_purchase_timestamp", kind="stable")
    if skip_orders > 0:
        orders = orders.iloc[skip_orders:]
    if max_orders > 0:
        orders = orders.head(max_orders)
    return orders


# Baca order items untuk order terpilih, lalu join kategori produk dari tabel products
def read_items(data_dir, order_ids):
    path = require_file(data_dir / ITEMS_FILE)
    items = pd.read_csv(path, parse_dates=["shipping_limit_date"])
    items = items[items["order_id"].isin(order_ids)]

    products_path = require_file(data_dir / PRODUCTS_FILE)
    products = pd.read_csv(products_path, usecols=["product_id", "product_category_name"])
    return items.merge(products, on="product_id", how="left")


# Baca order payments untuk order terpilih
def read_payments(data_dir, order_ids):
    path = require_file(data_dir / PAYMENTS_FILE)
    payments = pd.read_csv(path)
    return payments[payments["order_id"].isin(order_ids)]


# Peta order_id ke waktu beli, dipakai sebagai dasar timestamp event item dan payment
def build_purchase_times(orders):
    return {
        row.order_id: row.order_purchase_timestamp.to_pydatetime()
        for row in orders.itertuples(index=False)
    }


# Buat event order_created (offset 0 detik) dan order_status_changed (dilewati kalau kolom null)
def build_order_events(orders):
    events = []
    for row in orders.itertuples(index=False):
        purchase_time = row.order_purchase_timestamp.to_pydatetime()
        events.append(
            build_event(
                "order_created",
                purchase_time,
                (row.order_id,),
                {
                    "order_id": row.order_id,
                    "customer_id": row.customer_id,
                    "order_status": "created",
                },
            )
        )
        for column, status in STATUS_COLUMNS:
            status_time = to_datetime_or_none(getattr(row, column))
            if status_time is None:
                continue
            events.append(
                build_event(
                    "order_status_changed",
                    status_time,
                    (row.order_id, status),
                    {"order_id": row.order_id, "new_status": status},
                )
            )
    return events


# Buat event order_item dengan waktu = waktu beli + 1 detik + order_item_id dalam milidetik
def build_item_events(items, purchase_times):
    events = []
    for row in items.itertuples(index=False):
        item_id = int(row.order_item_id)
        event_time = purchase_times[row.order_id] + timedelta(seconds=1, milliseconds=item_id)
        shipping_limit = to_datetime_or_none(row.shipping_limit_date)
        events.append(
            build_event(
                "order_item",
                event_time,
                (row.order_id, item_id),
                {
                    "order_id": row.order_id,
                    "order_item_id": item_id,
                    "product_id": row.product_id,
                    "seller_id": row.seller_id,
                    "product_category_name": clean_text(row.product_category_name),
                    "price": float(row.price),
                    "freight_value": float(row.freight_value),
                    "shipping_limit_date": to_iso(shipping_limit) if shipping_limit else None,
                },
            )
        )
    return events


# Buat event payment dengan waktu = waktu beli + 2 detik + payment_sequential dalam milidetik
def build_payment_events(payments, purchase_times):
    events = []
    for row in payments.itertuples(index=False):
        sequential = int(row.payment_sequential)
        event_time = purchase_times[row.order_id] + timedelta(seconds=2, milliseconds=sequential)
        events.append(
            build_event(
                "payment",
                event_time,
                (row.order_id, sequential),
                {
                    "order_id": row.order_id,
                    "payment_sequential": sequential,
                    "payment_type": row.payment_type,
                    "payment_installments": int(row.payment_installments),
                    "payment_value": float(row.payment_value),
                },
            )
        )
    return events


# Entry point loader: kembalikan semua event Olist (belum diurutkan) untuk max_orders order pertama
def load_olist_events(data_dir, max_orders=0, skip_orders=0):
    data_dir = Path(data_dir)
    orders = read_orders(data_dir, max_orders, skip_orders)
    order_ids = set(orders["order_id"])
    purchase_times = build_purchase_times(orders)

    items = read_items(data_dir, order_ids)
    payments = read_payments(data_dir, order_ids)

    events = build_order_events(orders)
    events.extend(build_item_events(items, purchase_times))
    events.extend(build_payment_events(payments, purchase_times))
    return events
