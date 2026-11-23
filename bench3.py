"""真实调优对照：分区裁剪 / 列裁剪 / shuffle 分区数
这三个是离线数仓里最常用、也最容易被面试官追问的优化手段，全部可复现。
"""
import os, time, json
from pyspark.sql import SparkSession, functions as F

OUT = r"C:\bigdata-lab\warehouse"
TMP = r"C:\bigdata-lab\tmp"

def make(parts):
    return (
        SparkSession.builder.appName(f"prune_{parts}")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", str(parts))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", TMP)
        .getOrCreate()
    )

spark = make(200)
spark.sparkContext.setLogLevel("WARN")

ods = spark.read.parquet(f"{OUT}/ods_order")
dwd = (
    ods.withColumn("province", F.upper(F.trim(F.col("province"))))
       .filter(F.col("amount") > 0)
       .filter(F.col("order_id").isNotNull())
)

def run(label, df):
    t = time.time()
    n = df.count()
    sec = time.time() - t
    print(f"{label}\t{sec:.1f}s\t{n:,} rows")
    return {"label": label, "seconds": round(sec, 1), "rows": n}

AGG = lambda: (F.sum("amount").alias("gmv"), F.count("*").alias("cnt"))
DAY = "2026-09-15"

res = {}
res["q1_full_scan"] = run("Q1 全表 30 天 groupBy", 
    dwd.groupBy("dt", "merchant_id").agg(*AGG()))
res["q2_partition_prune"] = run("Q2 分区裁剪 单日 groupBy",
    dwd.filter(F.col("dt") == DAY).groupBy("dt", "merchant_id").agg(*AGG()))
res["q3_col_prune"] = run("Q3 分区裁剪 + 列裁剪 groupBy",
    dwd.filter(F.col("dt") == DAY).select("dt", "merchant_id", "amount")
       .groupBy("dt", "merchant_id").agg(*AGG()))

spark.stop()

# shuffle 分区数对比：同一查询，200 vs 20
res["shuffle_parts"] = {}
for parts in (200, 20):
    s = make(parts)
    s.sparkContext.setLogLevel("WARN")
    d = (s.read.parquet(f"{OUT}/ods_order")
          .filter(F.col("dt") == DAY)
          .filter(F.col("amount") > 0))
    t = time.time()
    n = d.groupBy("dt", "merchant_id").agg(F.sum("amount").alias("gmv")).count()
    sec = round(time.time() - t, 1)
    print(f"shuffle_parts={parts}\t{sec}s\t{n:,} rows")
    res["shuffle_parts"][str(parts)] = {"seconds": sec, "rows": n}
    s.stop()

with open(os.path.join(OUT, "prune_result.json"), "w", encoding="utf-8") as f:
    json.dump(res, f, ensure_ascii=False, indent=2)
print(json.dumps(res, ensure_ascii=False, indent=2))
