"""严格对照重测：同一份数据、同一个 SparkSession、同一条 SQL，各跑 3 次取中位数。

与 bench3.py 的区别（面试官会问的正是这点）：
  1. 不做"另起 session 换参数"——只在同一个 session 里 spark.conf.set 改 shuffle 分区数；
  2. shuffle 对照用的查询和 Q3 完全同一条（同 select、同过滤、同聚合），
     不像上次那样少了 count 聚合、多了 amount>0 过滤；
  3. 每个口径跑 3 次，报中位数，同时保留三次原始值，用来交代替数的波动范围。

输出：C:\\bigdata-lab\\warehouse\\prune_result_v2.json
"""
import json
import os
import statistics
import time

from pyspark.sql import SparkSession, functions as F

OUT = r"C:\bigdata-lab\warehouse"
TMP = r"C:\bigdata-lab\tmp"
DAY = "2026-09-15"
REPEAT = 3

spark = (
    SparkSession.builder.appName("strict_bench")
    .master("local[4]")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.enabled", "false")
    .config("spark.local.dir", TMP)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark", spark.version, "| master", spark.sparkContext.master,
      "| 默认并行度", spark.sparkContext.defaultParallelism)

ods = spark.read.parquet(f"{OUT}/ods_order")
dwd = (
    ods.withColumn("province", F.upper(F.trim(F.col("province"))))
       .filter(F.col("amount") > 0)
       .filter(F.col("order_id").isNotNull())
)

AGG = (F.sum("amount").alias("gmv"), F.count("*").alias("cnt"))


def run_timed(df, label, repeat=REPEAT):
    secs, rows = [], None
    for i in range(repeat):
        t = time.time()
        rows = df.count()
        secs.append(round(time.time() - t, 1))
        print(f"  {label} #{i+1}: {secs[-1]}s ({rows:,} rows)", flush=True)
    med = round(statistics.median(secs), 1)
    print(f"  -> {label}: 中位数 {med}s  原始 {secs}", flush=True)
    return {"label": label, "median_sec": med, "runs": secs, "rows": rows}


res = {"env": {
    "spark": spark.version,
    "master": spark.sparkContext.master,
    "java": spark.sparkContext._jvm.System.getProperty("java.version"),
    "driver_mem": "6g",
    "aqe": "false",
    "method": f"同一 session、同一 SQL，各跑 {REPEAT} 次取中位数",
}}

print("\n[1] Q1 全表 30 天聚合（无 dt 条件）", flush=True)
q1 = dwd.groupBy("dt", "merchant_id").agg(*AGG)
res["q1_full_scan"] = run_timed(q1, "Q1 全表 30 天 groupBy")

print("\n[2] Q2 加 dt 等值条件（分区裁剪）", flush=True)
q2 = dwd.filter(F.col("dt") == DAY).groupBy("dt", "merchant_id").agg(*AGG)
res["q2_partition_prune"] = run_timed(q2, "Q2 分区裁剪 单日 groupBy")

print("\n[3] Q3 再只读必要列（列裁剪）", flush=True)
q3 = (dwd.filter(F.col("dt") == DAY).select("dt", "merchant_id", "amount")
         .groupBy("dt", "merchant_id").agg(*AGG))
res["q3_col_prune"] = run_timed(q3, "Q3 分区裁剪 + 列裁剪")

# ---- shuffle 分区数：与 Q3 严格同一条 SQL，只改 conf
print("\n[4] shuffle 分区数对照（与 Q3 同一条 SQL）", flush=True)
res["shuffle_parts"] = {}
for parts in (200, 20):
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    res["shuffle_parts"][str(parts)] = run_timed(q3, f"Q3 shuffle={parts}")

res["shuffle_speedup"] = round(
    res["shuffle_parts"]["200"]["median_sec"] / res["shuffle_parts"]["20"]["median_sec"], 2)
res["prune_speedup"] = round(
    res["q1_full_scan"]["median_sec"] / res["q2_partition_prune"]["median_sec"], 2)
res["col_prune_speedup"] = round(
    res["q2_partition_prune"]["median_sec"] / res["q3_col_prune"]["median_sec"], 2)

with open(os.path.join(OUT, "prune_result_v2.json"), "w", encoding="utf-8") as f:
    json.dump(res, f, ensure_ascii=False, indent=2)

print("\n" + json.dumps(res, ensure_ascii=False, indent=2))
spark.stop()
