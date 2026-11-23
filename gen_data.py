"""生成离线数仓实验数据：商户维表 + 订单明细（含人为构造的 key 倾斜）
用法：
    python gen_data.py                # 默认 1.5 亿行
    python gen_data.py 20000000       # 2000 万行（先小规模试跑）
输出目录默认 C:\\bigdata-lab\\warehouse
"""
import sys, os, time
from pyspark.sql import SparkSession, functions as F

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 150_000_000
OUT = sys.argv[2] if len(sys.argv) > 2 else r"C:\bigdata-lab\warehouse"
TMP = r"C:\bigdata-lab\tmp"

for d in (OUT, TMP):
    os.makedirs(d, exist_ok=True)

spark = (
    SparkSession.builder.appName("gen_orders")
    .master("local[8]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.local.dir", TMP)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

t0 = time.time()

# 商户维表：10 万个商户，其中 id<1000 的 1000 个（1%）是大客户，承载 60% 订单
merchants = spark.range(0, 100_000).select(
    F.col("id").cast("int").alias("merchant_id"),
    F.concat(F.lit("M"), F.lpad(F.col("id").cast("string"), 6, "0")).alias("merchant_name"),
    F.when(F.col("id") < 1000, F.lit("KA")).otherwise(F.lit("SMB")).alias("merchant_level"),
)
merchants.write.mode("overwrite").parquet(f"{OUT}/dim_merchant")
print(f"[1/2] dim_merchant 写入完成 {time.time()-t0:.1f}s")

# 订单明细：随机列先物化，避免多次 rand() 重复求值
base = (
    spark.range(0, ROWS)
    .withColumn("u1", F.rand(42))
    .withColumn("u2", F.rand(43))
    .withColumn("u3", F.rand(44))
    .withColumn("u4", F.rand(45))
    .withColumn("u5", F.rand(46))
)

orders = (
    base.withColumn(
        "merchant_id",
        F.when(F.col("u1") < 0.6, (F.col("u2") * 1000).cast("int"))          # 热点：1% 商户吃 60% 订单
        .otherwise((F.col("u3") * 99000 + 1000).cast("int")),               # 长尾
    )
    .withColumn("order_id", F.col("id"))
    .withColumn("user_id", (F.col("u4") * 5_000_000).cast("int"))
    .withColumn("amount", F.round(F.col("u5") * 4990 + 10, 2))
    .withColumn("status", (F.col("u2") * 4).cast("int"))
    .withColumn("province", F.element_at(F.array(*[F.lit(p) for p in
        ["江苏", "浙江", "上海", "北京", "广东", "山东", "河南", "四川"]]),
        (F.col("u3") * 8).cast("int") + 1))
    .withColumn("dt", F.date_format(F.date_add(F.lit("2026-09-01").cast("date"),
        (F.col("u4") * 30).cast("int")), "yyyy-MM-dd"))
    .select("order_id", "merchant_id", "user_id", "amount", "status", "province", "dt")
)

orders.write.mode("overwrite").partitionBy("dt").parquet(f"{OUT}/ods_order")
print(f"[2/2] ods_order 写入完成，总耗时 {time.time()-t0:.1f}s")

# 校验倾斜是否构造成功
chk = orders.groupBy("merchant_id").count().orderBy(F.desc("count"))
top = chk.limit(1000).agg(F.sum("count").alias("hot")).collect()[0]["hot"]
print(f"校验：前 1000 个商户订单数 {top}，占比 {top/ROWS:.1%}")
spark.stop()
