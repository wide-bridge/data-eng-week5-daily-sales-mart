# mart.py 개선판 — 인자·로그·검증 추가
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

target_date = sys.argv[1] if len(sys.argv) > 1 else None
print(f"[mart] 대상 날짜: {target_date or '전체'}")

spark = (
    SparkSession.builder.appName("daily-sales-mart")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "s3a://warehouse/lake")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")   # INFO 로그를 줄여 결과 표가 끝에 보이게

orders = spark.createDataFrame(
    [(1, "P001", 2, "2026-08-24"), (2, "P002", 1, "2026-08-24"),
     (3, "P001", 5, "2026-08-25"), (4, "P003", 3, "2026-08-25"),
     (5, "P002", 2, "2026-08-26")],
    ["order_id", "product_id", "qty", "order_date"],
)
products = spark.createDataFrame(
    [("P001", "keyboard", 45000), ("P002", "mouse", 23000), ("P003", "monitor", 310000)],
    ["product_id", "name", "price"],
)

if target_date:
    orders = orders.filter(F.col("order_date") == target_date)

src_cnt = orders.count()
print(f"[mart] 원천 {src_cnt}행")
if src_cnt == 0:
    print("[mart] 원천이 비어 있습니다 — 중단")
    spark.stop()
    sys.exit(1)

mart = (orders.join(products, on="product_id")
        .withColumn("amount", F.col("qty") * F.col("price"))
        .groupBy("order_date")
        .agg(F.sum("amount").alias("daily_amount"),
             F.count("order_id").alias("order_cnt")))

out_cnt = mart.count()
print(f"[mart] 마트 {out_cnt}행")

mart.coalesce(1).writeTo("lake.db.daily_sales_mart").createOrReplace()
spark.sql("SELECT * FROM lake.db.daily_sales_mart ORDER BY order_date").show()
print("[mart] 완료")
spark.stop()

