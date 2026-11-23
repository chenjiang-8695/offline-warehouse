"""清洗规则验证：造一批 5 万行脏数据，跑 DWD 的同一套清洗逻辑，报每条规则命中多少。

为什么单独做这一份：1.5 亿行生成数据本身是干净的（没有负金额、没有空主键），
直接在上面跑清洗规则命中 0 行 —— 面试官问「清洗到底洗掉了什么」就没法答。
所以另外造一批脏样本，把 rules 的命中数、输出行数都记下来。

规则（与 pipeline_warehouse.py 的 DWD 一致）：
  1. amount <= 0 过滤
  2. order_id 为空过滤
  3. order_id 重复去重（保留第一条）
  4. province 前后空格 trim

产出：C:\\bigdata-lab\\warehouse\\cleaning_result.json
"""
import json
import os

from pyspark.sql import SparkSession, functions as F

OUT = r"C:\bigdata-lab\warehouse"
TMP = r"C:\bigdata-lab\tmp"
N = 50_000

spark = (
    SparkSession.builder.appName("clean_rules")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.enabled", "false")
    .config("spark.local.dir", TMP)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw0 = spark.range(0, N).select(F.col("id").alias("seq"),
                                F.col("id").cast("long").alias("order_id"))
raw = (raw0
       .withColumn("order_id",
                   F.when(F.pmod(F.col("order_id"), F.lit(1000)) == 7, F.lit(None).cast("long"))
                    .when(F.pmod(F.col("order_id"), F.lit(25)) == 0, F.col("order_id") - 1)
                    .otherwise(F.col("order_id")))
       .withColumn("amount",
                   F.when(F.pmod(F.col("seq"), F.lit(100)) < 6, F.lit(-10.0))
                    .otherwise(F.round(F.rand(1) * 4990 + 10, 2)))
       .withColumn("province",
                   F.when(F.pmod(F.col("seq"), F.lit(20)) == 0, F.lit("  江苏 "))
                    .otherwise(F.lit("江苏")))
       .withColumn("merchant_id", (F.pmod(F.col("seq"), F.lit(2000))).cast("int"))
       .withColumn("dt", F.lit("2026-09-15"))
       .drop("seq"))

total = raw.count()
neg = raw.filter(F.col("amount") <= 0).count()
null_oid = raw.filter(F.col("order_id").isNull()).count()
blank_prov = raw.filter(F.col("province") != F.trim(F.col("province"))).count()

cleaned = (raw.withColumn("province", F.trim(F.col("province")))
              .filter(F.col("amount") > 0)
              .filter(F.col("order_id").isNotNull()))
after_filters = cleaned.count()
dedup = cleaned.dropDuplicates(["order_id"])
after_dedup = dedup.count()
dup_removed = after_filters - after_dedup

res = {
    "input_rows": total,
    "rule_hits": {"amount_le_0": neg, "order_id_null": null_oid,
                  "province_whitespace": blank_prov, "order_id_duplicate": dup_removed},
    "after_filters": after_filters,
    "output_rows": after_dedup,
    "dropped_total": total - after_dedup,
    "province_distinct_after": dedup.select("province").distinct().count(),
}
print(json.dumps(res, ensure_ascii=False, indent=2), flush=True)
with open(os.path.join(OUT, "cleaning_result.json"), "w", encoding="utf-8") as f:
    json.dump(res, f, ensure_ascii=False, indent=2)
spark.stop()
