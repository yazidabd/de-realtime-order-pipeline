// Pilih database target dari env compose, fallback ke orders_pipeline
const dbName = process.env.MONGO_INITDB_DATABASE || "orders_pipeline";
const target = db.getSiblingDB(dbName);

// Buat collection kalau belum ada, supaya script aman dijalankan ulang
function ensureCollection(name) {
  if (!target.getCollectionNames().includes(name)) {
    target.createCollection(name);
  }
}

// Buat semua index untuk collection serving dan ledger
function createIndexes() {
  target.orders_current.createIndex({ customer_id: 1 });
  target.orders_current.createIndex({ status: 1 });
  target.orders_current.createIndex({ updated_at: -1 });

  target.metrics_window.createIndex(
    { window_start: 1, event_type: 1 },
    { unique: true }
  );

  target.stock_current.createIndex({ updated_at: -1 });

  // Ledger event_id yang sudah dihitung ke metrics_window, kadaluarsa 30 hari
  target.processed_events.createIndex(
    { processed_at: 1 },
    { expireAfterSeconds: 2592000 }
  );
}

["orders_current", "metrics_window", "stock_current", "processed_events"].forEach(ensureCollection);
createIndexes();
print("init_indexes selesai untuk database " + dbName);
