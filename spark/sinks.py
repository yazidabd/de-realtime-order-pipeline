# Sink MongoDB lewat pymongo, dipanggil dari foreachBatch di streaming_job.py
import os
from datetime import datetime, timezone
from urllib.parse import quote_plus

from pymongo import MongoClient, UpdateOne

ORDER_EVENT_TYPES = ("order_created", "order_status_changed")

_client = None

# SQL agregasi per micro-batch: jumlah event dan total nilai per window publish_time dan tipe event
METRICS_SQL = """
SELECT
    date_format(window.start, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS window_start,
    date_format(window.end, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS window_end,
    event_type,
    COUNT(*) AS event_count,
    ROUND(SUM(COALESCE(payment_value, 0)), 2) AS payment_total,
    ROUND(SUM(COALESCE(price, 0)), 2) AS item_total
FROM batch_events
GROUP BY window(publish_ts, '{window}'), event_type
"""


# Ambil client MongoDB replica set, dibuat sekali lalu dipakai ulang di semua micro-batch
def get_client():
    global _client
    if _client is None:
        user = quote_plus(os.environ["MONGO_USER"])
        password = quote_plus(os.environ["MONGO_PASSWORD"])
        host = os.environ.get("MONGO_HOST", "mongodb")
        port = os.environ.get("MONGO_PORT", "27017")
        replica_set = os.environ.get("MONGO_REPLICA_SET", "rs0")
        uri = (
            f"mongodb://{user}:{password}@{host}:{port}/"
            f"?authSource=admin&replicaSet={replica_set}"
        )
        _client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    return _client


# Ambil database target dari client
def get_database():
    return get_client()[os.environ.get("MONGO_DB", "orders_pipeline")]


# Susun isi stage $set dengan aturan latest wins: field hanya ditimpa kalau stamp event lebih baru dari updated_at yang tersimpan
def latest_wins_stage(stamp, fields):
    newer = {"$gt": [{"$literal": stamp}, {"$ifNull": ["$updated_at", ""]}]}
    stage = {
        name: {"$cond": [newer, {"$literal": value}, "$" + name]}
        for name, value in fields.items()
    }
    stage["updated_at"] = {"$cond": [newer, {"$literal": stamp}, "$updated_at"]}
    return stage


# Tulis satu micro-batch event bersih ke orders_current dan stock_current, diurutkan menurut event_time
def write_serving_batch(batch_df, batch_id):
    rows = batch_df.select(
        "event_type",
        "event_time",
        "order_id",
        "customer_id",
        "order_status_value",
        "product_id",
        "warehouse_id",
        "stock_level",
    ).collect()
    if not rows:
        return

    rows.sort(key=lambda row: row["event_time"])
    order_ops = []
    stock_ops = []

    for row in rows:
        if row["event_type"] in ORDER_EVENT_TYPES:
            stage = latest_wins_stage(row["event_time"], {"status": row["order_status_value"]})
            if row["customer_id"] is not None:
                stage["customer_id"] = {
                    "$ifNull": ["$customer_id", {"$literal": row["customer_id"]}]
                }
            order_ops.append(UpdateOne({"_id": row["order_id"]}, [{"$set": stage}], upsert=True))
        elif row["event_type"] == "stock_updated":
            stage = latest_wins_stage(
                row["event_time"],
                {"stock_level": row["stock_level"], "warehouse_id": row["warehouse_id"]},
            )
            stock_ops.append(UpdateOne({"_id": row["product_id"]}, [{"$set": stage}], upsert=True))

    database = get_database()
    if order_ops:
        database.orders_current.bulk_write(order_ops, ordered=True)
    if stock_ops:
        database.stock_current.bulk_write(stock_ops, ordered=True)


# Ambil event_id dari daftar yang sudah tercatat di ledger processed_events
def find_processed_ids(database, event_ids):
    cursor = database.processed_events.find({"_id": {"$in": event_ids}}, {"_id": 1})
    return {doc["_id"] for doc in cursor}


# Buang event yang sudah ada di ledger lewat left anti join
def drop_processed(batch_df, processed_ids):
    if not processed_ids:
        return batch_df
    processed_df = batch_df.sparkSession.createDataFrame(
        [(event_id,) for event_id in processed_ids], "event_id string"
    )
    return batch_df.join(processed_df, on="event_id", how="left_anti")


# Tulis ledger dan agregasi window dalam satu transaksi, jadi retry atau replay batch tidak pernah menghitung dua kali
def commit_metrics(ledger_docs, operations):
    database = get_database()

    def callback(session):
        database.processed_events.insert_many(ledger_docs, ordered=True, session=session)
        if operations:
            database.metrics_window.bulk_write(operations, ordered=True, session=session)

    with get_client().start_session() as session:
        session.with_transaction(callback)


FAULT_MARKER = "/checkpoints/.fault_after_commit_done"


# Hook uji kegagalan: bila FAULT_CRASH_AFTER_COMMIT=1, matikan proses satu kali tepat setelah transaksi commit dan sebelum checkpoint
def maybe_crash_after_commit(batch_id):
    if os.environ.get("FAULT_CRASH_AFTER_COMMIT") != "1":
        return
    if os.path.exists(FAULT_MARKER):
        return
    open(FAULT_MARKER, "w").close()
    print(f"fault injection: crash setelah commit batch {batch_id}", flush=True)
    os._exit(137)


# Buat fungsi foreachBatch untuk metrics_window: cek ledger, agregasi pakai Spark SQL, lalu commit transaksional
def make_metrics_writer(window_duration):
    sql = METRICS_SQL.format(window=window_duration)

    def write_metrics_batch(batch_df, batch_id):
        unique_df = batch_df.dropDuplicates(["event_id"]).persist()
        fresh_df = None
        try:
            event_ids = [row["event_id"] for row in unique_df.select("event_id").collect()]
            if not event_ids:
                return

            processed_ids = find_processed_ids(get_database(), event_ids)
            fresh_df = drop_processed(unique_df, processed_ids).persist()
            fresh_ids = [row["event_id"] for row in fresh_df.select("event_id").collect()]
            print(
                f"metrics batch {batch_id}: {len(event_ids)} event, "
                f"{len(fresh_ids)} baru, {len(processed_ids)} dilewati ledger",
                flush=True,
            )
            if not fresh_ids:
                return

            fresh_df.createOrReplaceTempView("batch_events")
            rows = fresh_df.sparkSession.sql(sql).collect()
            operations = [
                UpdateOne(
                    {"window_start": row["window_start"], "event_type": row["event_type"]},
                    {
                        "$inc": {
                            "event_count": int(row["event_count"]),
                            "payment_total": float(row["payment_total"]),
                            "item_total": float(row["item_total"]),
                        },
                        "$set": {"window_end": row["window_end"]},
                    },
                    upsert=True,
                )
                for row in rows
            ]
            now = datetime.now(timezone.utc)
            ledger_docs = [
                {"_id": event_id, "processed_at": now, "batch_id": int(batch_id)}
                for event_id in fresh_ids
            ]
            commit_metrics(ledger_docs, operations)
            maybe_crash_after_commit(batch_id)
        finally:
            unique_df.unpersist()
            if fresh_df is not None:
                fresh_df.unpersist()

    return write_metrics_batch
