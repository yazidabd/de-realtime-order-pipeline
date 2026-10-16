# Hitung kondisi landing zone dan MongoDB untuk failure drill, dijalankan di dalam container spark
import glob
import json
import os
from datetime import datetime
from urllib.parse import quote_plus

from pymongo import MongoClient

from schemas import NON_NEGATIVE_FIELDS, REQUIRED_FIELDS

LANDING_GLOB = "/landing/orders-*.json"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


# Cek nilai terisi: bukan None dan bukan string kosong
def is_present(value):
    return value is not None and str(value).strip() != ""


# Cek string waktu bisa diparse dengan format event
def parses_as_time(value):
    try:
        datetime.strptime(value, TIME_FORMAT)
        return True
    except (TypeError, ValueError):
        return False


# Tiru aturan validasi Spark: tipe dikenal, field wajib terisi, waktu valid, nilai uang tidak negatif
def is_valid_event(event):
    event_type = event.get("event_type")
    if event_type not in REQUIRED_FIELDS:
        return False
    required = ("event_id", "event_time") + tuple(REQUIRED_FIELDS[event_type])
    if not all(is_present(event.get(field)) for field in required):
        return False
    if not parses_as_time(event.get("event_time")) or not parses_as_time(event.get("publish_time")):
        return False
    for field in NON_NEGATIVE_FIELDS:
        value = event.get(field)
        if isinstance(value, (int, float)) and value < 0:
            return False
    return True


# Baca semua file landing, kembalikan jumlah file, jumlah baris, dan himpunan event_id yang valid
def scan_landing():
    files = sorted(glob.glob(LANDING_GLOB))
    lines = 0
    valid_ids = set()
    for path in files:
        with open(path, encoding="utf-8") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                lines += 1
                event = json.loads(raw)
                if is_valid_event(event):
                    valid_ids.add(event["event_id"])
    return len(files), lines, valid_ids


# Ambil hitungan dari MongoDB: ledger, total event_count metrics_window, dan jumlah dokumen serving
def read_mongo():
    user = quote_plus(os.environ["MONGO_USER"])
    password = quote_plus(os.environ["MONGO_PASSWORD"])
    host = os.environ.get("MONGO_HOST", "mongodb")
    port = os.environ.get("MONGO_PORT", "27017")
    uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin&replicaSet=rs0"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    database = client[os.environ.get("MONGO_DB", "orders_pipeline")]
    totals = list(
        database.metrics_window.aggregate([{"$group": {"_id": None, "total": {"$sum": "$event_count"}}}])
    )
    return {
        "ledger": database.processed_events.count_documents({}),
        "metrics_total": totals[0]["total"] if totals else 0,
        "orders_current": database.orders_current.count_documents({}),
        "stock_current": database.stock_current.count_documents({}),
    }


# Cetak snapshot: angka landing dan MongoDB, lalu verdict consistent
def main():
    files, lines, valid_ids = scan_landing()
    print(f"landing_files={files}")
    print(f"landing_lines={lines}")
    print(f"expected_valid_ids={len(valid_ids)}")
    try:
        counts = read_mongo()
    except Exception as error:
        print(f"mongo_error={type(error).__name__}")
        print("consistent=NO")
        return
    print(f"mongo_ledger={counts['ledger']}")
    print(f"mongo_metrics_total={counts['metrics_total']}")
    print(f"mongo_orders_current={counts['orders_current']}")
    print(f"mongo_stock_current={counts['stock_current']}")
    consistent = counts["ledger"] == len(valid_ids) and counts["metrics_total"] == counts["ledger"]
    print(f"consistent={'YES' if consistent else 'NO'}")


main()
