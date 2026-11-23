"""离线数仓主链路：ODS -> DWD（清洗 + 维表关联 + 未知商户打标）-> DWS -> ADS

与 bench3.py 的「只读 + groupBy」对照实验不同，这一版是真落盘的四层链路：
每层的行数、耗时、脏数据量与未知商户占比都写进 warehouse_result.json，
用来回答「1.5 亿行怎么来的、清洗掉了多少、25% 匹配不到商户怎么处理」。

维表口径：dim_merchant 有 10 万商户，但只覆盖其中 7.5 万（模拟维表更新滞后 /
埋点缺失），订单里约有 25% 的 merchant_id 在维表里找不到 —— 这类订单不丢弃，
打标 UNKNOWN 后照常进 DWS，日报里单独统计未知商户占比。

运行：py -3.14 pipeline_warehouse.py
产出：warehouse/dwd_order、dws_merchant_dt、ads_daily_summary、ads_top_merchant
"""
import json
import os
import time

from pyspark.sql import SparkSession, functions as F, Window

OUT = r"C:\bigdata-lab\warehouse"
TMP = r"C:\bigdata-lab\tmp"
UNKNOWN = "UNKNOWN"
DIM_COVERAGE = 75  # 维表只覆盖 75% 的商户号

spark = (
    SparkSession.builder.appName("warehouse_etl")
    .master("local[4]")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "40")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.enabled", "false")
    .config("spark.local.dir", TMP)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark", spark.version, "| master", spark.sparkContext.master, flush=True)

stats = {"env": {"spark": spark.version, "master": spark.sparkContext.master,
                 "java": spark.sparkContext._jvm.System.getProperty("java.version"),
                 "driver_mem": "6g", "shuffle_partitions": 40},
         "dim_coverage": f"{DIM_COVERAGE}%"}

t0 = time.time()
ods = spark.read.parquet(f"{OUT}/ods_order")
total = ods.count()
neg = ods.filter(F.col("amount") <= 0).count()
no_order = ods.filter(F.col("order_id").isNull()).count()
stats["ods"] = {"rows": total, "negative_amount": neg, "null_order_id": no_order,
                "seconds": round(time.time() - t0, 1)}
print(f"[1/4] ODS {total:,} 行 | 负金额 {neg:,} | 空订单号 {no_order:,}", flush=True)

# ---------------- DWD：维表关联 + 清洗 + 打标（不丢行）
t1 = time.time()
dim = spark.read.parquet(f"{OUT}/dim_merchant")
dim_all = dim.count()
dim_key = (dim.filter(F.pmod(F.col("merchant_id") * 7, F.lit(100)) < DIM_COVERAGE)
           .select("merchant_id",
                   F.concat(F.lit("M"), F.lpad(F.col("merchant_id").cast("string"), 6, "0"))
                    .alias("merchant_key"),
                   F.col("merchant_level")))
dim_covered = dim_key.count()

dwd = (
    ods.join(F.broadcast(dim_key), "merchant_id", "left")
       .withColumn("merchant_key", F.coalesce(F.col("merchant_key"), F.lit(UNKNOWN)))
       .withColumn("merchant_level", F.coalesce(F.col("merchant_level"), F.lit(UNKNOWN)))
       .withColumn("province", F.trim(F.col("province")))
       .filter(F.col("amount") > 0)
       .filter(F.col("order_id").isNotNull())
       .select("order_id", "merchant_key", "merchant_level", "user_id",
               "amount", "status", "province", "dt")
)
dwd.write.mode("overwrite").partitionBy("dt").parquet(f"{OUT}/dwd_order")
dwd_df = spark.read.parquet(f"{OUT}/dwd_order")
dwd_rows = dwd_df.count()
unknown_rows = dwd_df.filter(F.col("merchant_key") == UNKNOWN).count()
stats["dwd"] = {"rows": dwd_rows, "dropped": total - dwd_rows,
                "dim_merchants": dim_all, "dim_covered": dim_covered,
                "unknown_merchant_rows": unknown_rows,
                "unknown_share": round(unknown_rows / dwd_rows, 4),
                "seconds": round(time.time() - t1, 1)}
print(f"[2/4] DWD {dwd_rows:,} 行（丢弃 {total-dwd_rows:,}）| 维表覆盖 "
      f"{dim_covered:,}/{dim_all:,} | 未知商户 {unknown_rows:,} 行 "
      f"({unknown_rows/dwd_rows:.2%}) 保留并打标", flush=True)

# ---------------- DWS：按 dt + 商户聚合
t2 = time.time()
dws = (dwd_df.groupBy("dt", "merchant_key")
       .agg(F.sum("amount").alias("gmv"), F.count("*").alias("order_cnt")))
dws.write.mode("overwrite").partitionBy("dt").parquet(f"{OUT}/dws_merchant_dt")
dws_df = spark.read.parquet(f"{OUT}/dws_merchant_dt")
dws_rows = dws_df.count()
stats["dws"] = {"rows": dws_rows, "seconds": round(time.time() - t2, 1)}
print(f"[3/4] DWS {dws_rows:,} 行", flush=True)

# ---------------- ADS：日报 + 每日 Top10 商户
t3 = time.time()
daily = dws_df.groupBy("dt").agg(
    F.round(F.sum("gmv"), 2).alias("gmv"),
    F.sum("order_cnt").alias("order_cnt"),
    F.sum(F.when(F.col("merchant_key") == UNKNOWN, F.col("order_cnt"))
           .otherwise(F.lit(0))).alias("unknown_order_cnt"),
)
daily.write.mode("overwrite").parquet(f"{OUT}/ads_daily_summary")
daily.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/ads_daily_csv")

top = (dws_df.withColumn("rn", F.row_number().over(
            Window.partitionBy("dt").orderBy(F.desc("gmv"))))
       .filter(F.col("rn") <= 10))
top.write.mode("overwrite").partitionBy("dt").parquet(f"{OUT}/ads_top_merchant")

total_orders = dws_df.agg(F.sum("order_cnt")).collect()[0][0]
unknown_orders = daily.agg(F.sum("unknown_order_cnt")).collect()[0][0]
stats["ads"] = {"daily_rows": daily.count(), "top_rows": top.count(),
                "unknown_share_overall": round(unknown_orders / total_orders, 4),
                "gmv_total": float(daily.agg(F.sum("gmv")).collect()[0][0]),
                "seconds": round(time.time() - t3, 1)}
stats["total_seconds"] = round(time.time() - t0, 1)

with open(os.path.join(OUT, "warehouse_result.json"), "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)
print("[4/4] ADS 完成", flush=True)
print(json.dumps(stats, ensure_ascii=False, indent=2), flush=True)
spark.stop()
