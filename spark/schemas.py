from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Skema gabungan semua tipe event. Field yang tidak relevan untuk suatu tipe akan bernilai null
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),
        StructField("publish_time", StringType()),
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("new_status", StringType()),
        StructField("order_item_id", LongType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("price", DoubleType()),
        StructField("freight_value", DoubleType()),
        StructField("shipping_limit_date", StringType()),
        StructField("payment_sequential", LongType()),
        StructField("payment_type", StringType()),
        StructField("payment_installments", LongType()),
        StructField("payment_value", DoubleType()),
        StructField("warehouse_id", StringType()),
        StructField("change_type", StringType()),
        StructField("quantity_change", LongType()),
        StructField("stock_level", LongType()),
    ]
)

# Field wajib per tipe event, sama dengan PAYLOAD_FIELDS di producer/event_schema.py
REQUIRED_FIELDS = {
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
