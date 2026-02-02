import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ========= EDIT THESE PATHS =========
BASE_DIR = r"C:\Users\Gahigi\Desktop\ecommerce-analytics\data_raw"
OUT_DIR  = r"C:\Users\Gahigi\Desktop\ecommerce-analytics\outputs"

TXN_PATH = os.path.join(BASE_DIR, "transactions.json")
PROD_PATH = os.path.join(BASE_DIR, "products.json")
CAT_PATH  = os.path.join(BASE_DIR, "categories.json")

os.makedirs(OUT_DIR, exist_ok=True)

# ========= 8GB RAM FRIENDLY SPARK CONFIG =========
spark = (
    SparkSession.builder
    .appName("ECOM_Spark_Batch_Easy")
    .master("local[2]")  # matches local[2] requirement idea :contentReference[oaicite:4]{index=4}
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ========= LOAD JSON (safe settings) =========
tx = spark.read.option("multiline", "true").json(TXN_PATH)
pr = spark.read.option("multiline", "true").json(PROD_PATH)
cat = spark.read.option("multiline", "true").json(CAT_PATH)

# Keep only needed columns to reduce memory
pr = pr.select("product_id", F.col("name").alias("product_name"), "category_id")
cat = cat.select("category_id", F.col("name").alias("category_name"))

# ========= NORMALIZE TRANSACTIONS (explode line items) =========
items = (
    tx.select("transaction_id", "user_id", "timestamp", "total", "status", F.explode("items").alias("it"))
      .select(
          "transaction_id", "user_id", "timestamp", "total", "status",
          F.col("it.product_id").alias("product_id"),
          F.col("it.quantity").alias("qty"),
          F.col("it.unit_price").alias("unit_price"),
          F.col("it.subtotal").alias("line_subtotal")
      )
)

# Optional cleanup
items = items.filter(F.col("status").isNotNull())  # keep all statuses, but avoid null noise

# Join product + category names
items_enriched = (
    items.join(pr, "product_id", "left")
         .join(cat, "category_id", "left")
         .withColumn("revenue", F.col("line_subtotal"))
)

items_enriched.cache()

# ============================================================
# 1) Revenue by category (Top 10)  :contentReference[oaicite:5]{index=5}
# ============================================================
rev_by_cat = (
    items_enriched.groupBy("category_id", "category_name")
    .agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.sum("qty").alias("units"),
        F.countDistinct("transaction_id").alias("orders")
    )
    .orderBy(F.desc("revenue"))
)

rev_by_cat_top10 = rev_by_cat.limit(10)
rev_by_cat_top10.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(OUT_DIR, "revenue_by_category_top10_csv"))

# ============================================================
# 2) Units sold by category (Top 10)
# ============================================================
units_by_cat_top10 = (
    items_enriched.groupBy("category_id", "category_name")
    .agg(F.sum("qty").alias("units"))
    .orderBy(F.desc("units"))
    .limit(10)
)

units_by_cat_top10.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(OUT_DIR, "units_sold_by_category_top10_csv"))

# ============================================================
# 3) Top spenders (Top 10 users by total spent)  :contentReference[oaicite:6]{index=6}
# ============================================================
# Use transaction totals (one per transaction), so compute from tx directly to avoid double-counting
tx_totals = tx.select("transaction_id", "user_id", F.col("total").cast("double").alias("total")).na.drop(subset=["user_id", "total"])

top_spenders = (
    tx_totals.groupBy("user_id")
    .agg(
        F.round(F.sum("total"), 2).alias("total_spent"),
        F.count("*").alias("orders")
    )
    .orderBy(F.desc("total_spent"))
    .limit(10)
)

top_spenders.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(OUT_DIR, "top_spenders_top10_csv"))

# ============================================================
# 4) Top users by number of orders (Top 10)
# ============================================================
top_users_by_orders = (
    tx_totals.groupBy("user_id")
    .agg(F.count("*").alias("orders"))
    .orderBy(F.desc("orders"))
    .limit(10)
)

top_users_by_orders.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(OUT_DIR, "top_users_by_orders_top10_csv"))

# ============================================================
# 5) Frequently bought together (Top 10 product pairs)  :contentReference[oaicite:7]{index=7}
# ============================================================
# Build pairs inside each transaction: (p1, p2) where p1 < p2
tprod = items_enriched.select("transaction_id", "product_id", "product_name").distinct()

a = tprod.alias("a")
b = tprod.alias("b")

pairs = (
    a.join(b, F.col("a.transaction_id") == F.col("b.transaction_id"))
     .where(F.col("a.product_id") < F.col("b.product_id"))
     .select(
         F.col("a.product_id").alias("product_a"),
         F.col("a.product_name").alias("product_a_name"),
         F.col("b.product_id").alias("product_b"),
         F.col("b.product_name").alias("product_b_name"),
     )
)

pair_counts = (
    pairs.groupBy("product_a", "product_a_name", "product_b", "product_b_name")
    .agg(F.count("*").alias("co_count"))
    .orderBy(F.desc("co_count"))
    .limit(10)
)

pair_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(OUT_DIR, "also_bought_pairs_top10_csv"))

items_enriched.unpersist()

print("\nDONE ✅ Spark outputs saved in:", OUT_DIR)
spark.stop()
