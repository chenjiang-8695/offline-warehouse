# -*- coding: utf-8 -*-
"""明天共享屏幕用的最小演示（Spark 3.5.3 local 模式）

跑法：双击 run_demo.bat，或命令行 py -3.14 demo_share.py
前提：HADOOP_HOME 指向 C:/Users/26756/opt/hadoop（bat 里已设好）

只演示四件"能在执行计划上看到证据"的事：
  1. 按 dt 分区写 Parquet
  2. 分区裁剪 -> PartitionFilters
  3. 列裁剪   -> ReadSchema 只剩选中的列
  4. 窗口函数 row_number 做每组 TopN
注意：数据量小（120 万行），耗时差异会被调度开销淹没，所以不比秒数只比"读了多少"。
      这也是面试里最稳的说法：小数据看计划，大数据才看耗时。
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "demo_data").replace("\\", "/")
OUT = os.path.join(BASE, "demo_out").replace("\\", "/")

spark = (
    SparkSession.builder.appName("warehouse-demo")
    .master("local[8]")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

print("=" * 66)
print("环境：Spark %s | master %s | 并行度 %d | Java %s"
      % (spark.version, sc.master, sc.defaultParallelism,
         sc._jvm.System.getProperty("java.version")))
print("=" * 66)

# ---------------------------------------------------------------- 1. 造数据 + 分区落盘
DAYS = ["2026-11-09", "2026-11-10", "2026-11-11"]
ROWS = 400000

t0 = time.time()
df = spark.range(0, ROWS).select(
    F.col("id").alias("order_id"),
    F.concat(F.lit("M"), F.lpad((F.col("id") % 2000).cast("string"), 5, "0")).alias("merchant_id"),
    ((F.col("id") % 900) + (F.col("id") % 7) / 10.0).cast("decimal(12,2)").alias("amount"),
    F.element_at(F.array(*[F.lit(d) for d in DAYS]),
                 (F.col("id") % len(DAYS) + 1).cast("int")).alias("dt"),
).cache()
total = df.count()
df.write.mode("overwrite").partitionBy("dt").parquet(DATA)
print("\n[1] ODS：%s 行源数据，按 dt 分区写 Parquet，用时 %.1fs"
      % (format(total, ","), time.time() - t0))
dirs = [d for d in sorted(os.listdir(DATA)) if d.startswith("dt=")]
print("    分区目录:", dirs)
size = sum(os.path.getsize(os.path.join(r, f))
           for r, _, fs in os.walk(DATA) for f in fs)
print("    落盘大小: %.1f MB" % (size / 1024 / 1024))

src = spark.read.parquet(DATA)
one_day = src.filter(F.col("dt") == "2026-11-11")

# ---------------------------------------------------------------- 2. 分区裁剪
print("\n" + "-" * 66)
print("[2] 分区裁剪：where dt = '2026-11-11'，看执行计划里的 PartitionFilters")
one_day.explain()
print("\n    单日行数 %s ／ 全表行数 %s ／ 只读 1 个分区目录（共 %d 个）"
      % (format(one_day.count(), ","), format(src.count(), ","), len(dirs)))
print("    说人话：分区列带等值条件才只扫对应目录；对 dt 套函数（如 date_format）裁剪直接失效。")

# ---------------------------------------------------------------- 3. 列裁剪
print("\n" + "-" * 66)
print("[3] 列裁剪：只 select 两列，ReadSchema 里就只剩这两列")
one_day.select("merchant_id", "amount").explain()

# ---------------------------------------------------------------- 4. DWS 聚合 + 窗口 TopN
print("\n" + "-" * 66)
print("[4] DWS 聚合 + 窗口函数取每日 Top3")
dws = src.groupBy("dt", "merchant_id").agg(
    F.sum("amount").alias("gmv"), F.count("*").alias("order_cnt"))
dws.createOrReplaceTempView("dws_merchant_gmv")
spark.sql("""
    select dt, merchant_id, gmv, order_cnt, rn
    from (
      select dt, merchant_id, gmv, order_cnt,
             row_number() over (partition by dt order by gmv desc) as rn
      from dws_merchant_gmv
    ) t
    where rn <= 3
    order by dt, rn
""").show(truncate=False)

dws.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT)
print("[5] ADS：聚合结果落盘 %s（生产里这一步 export 到 MySQL）" % OUT)

spark.stop()
print("\n" + "=" * 66)
print("总结三句（照着说）：")
print("1. 我这里是本地 local[8]，不是集群；数据也是自己造的，够验证分区/列裁剪就行。")
print("2. 分区裁剪看 PartitionFilters，列裁剪看 ReadSchema——数据小的时候看计划，不看秒数。")
print("3. 每组 TopN 用 row_number() over (partition by dt order by gmv desc)，不是 limit。")
