"""DWD 拉链表：商户状态历史（start_date / end_date），支持任意历史日期快照回溯。

本机没有 Hive，维表落 Parquet（与 Hive 分区表等价的组织方式）。

做法分两段，面试时说清分工：
  1. 回填（build_zip_rle）：读 30 天快照，按 merchant_id 排序，用 lag(level)
     找出状态变化点，再按变化点累计编号分组，把连续相同状态压成一个区间
     （start_date = 区间首日，end_date = 区间末日 + 1，最后一段留 9999-12-31）。
  2. 每日增量（update_one_day）：生产上每天跑这一段——拿当日快照与
     end_date = 9999-12-31 的开链记录比对，变化的闭旧链开新链，新商户直接
     开链，未变化的不动。脚本用第 30 天数据真跑一次，并与回填结果逐条比对。

验证：5 个历史日期各反查一次「当日有效记录」，与当日快照逐商户比对，
不一致条数应为 0；同时检查没有商户在同一天出现两条有效记录。

产出：C:\\bigdata-lab\\warehouse\\dim_merchant_zip + zip_result.json
"""
import json
import os
import time
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F, Window

OUT = r"C:\bigdata-lab\warehouse"
TMP = r"C:\bigdata-lab\tmp"
SNAP = f"{OUT}/dim_merchant_snapshots"
START = date(2026, 9, 1)
DAYS = 30
BASE_MERCHANTS = 100_000
NEW_PER_DAY = 200
OPEN = "9999-12-31"

spark = (
    SparkSession.builder.appName("zip_dim")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.enabled", "false")
    .config("spark.local.dir", TMP)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark", spark.version, flush=True)


def day_of(i):
    return (START + timedelta(days=i)).isoformat()


def make_snapshots():
    """30 天商户快照：10 万老商户（id<1000 的 1% 为 KA）+ 每天新增 200 个。
    换档确定性伪随机（hash(merchant_id*31 + day) % 100 < 3），约 3%/天；
    换档次数跨天累计，保证同一商户只在换档当天变一次、重跑结果一致。"""
    state = spark.range(0, 0).select(
        F.col("id").cast("integer").alias("merchant_id"), F.lit(0).alias("flip_cnt"))
    t0 = time.time()
    for d in range(DAYS):
        span = BASE_MERCHANTS + NEW_PER_DAY * d
        pop = spark.range(0, span).select(F.col("id").cast("integer").alias("merchant_id"))
        merged = pop.join(state, "merchant_id", "left").fillna(0, subset=["flip_cnt"])
        stepped = (
            merged.withColumn(
                "flip_cnt",
                F.col("flip_cnt") + F.when(
                    F.pmod(F.hash(F.col("merchant_id") * 31 + F.lit(d)), F.lit(100)) < 3,
                    F.lit(1)).otherwise(F.lit(0)))
            .withColumn("base_rank", F.when(F.col("merchant_id") < 1000, F.lit(1)).otherwise(F.lit(0)))
            .withColumn("level",
                        F.when(F.pmod(F.col("flip_cnt") + F.col("base_rank"), F.lit(2)) == 1,
                               F.lit("KA")).otherwise(F.lit("SMB")))
        )
        state = stepped.select("merchant_id", "flip_cnt").localCheckpoint(eager=True)
        (stepped.withColumn("dt", F.lit(day_of(d)))
         .select("merchant_id", "level", "dt")
         .write.mode("overwrite").parquet(f"{SNAP}/dt={day_of(d)}"))
    print(f"[1/4] 30 天快照生成 {time.time()-t0:.1f}s", flush=True)


def build_zip_rle(snap_df):
    """变更点压缩：连续相同状态合并为一个区间。"""
    w = Window.partitionBy("merchant_id").orderBy("dt")
    last_day = snap_df.agg(F.max("dt")).collect()[0][0]
    marked = (snap_df
              .withColumn("prev_level", F.lag("level").over(w))
              .withColumn("is_change",
                          F.when(F.col("prev_level").isNull()
                                 | (F.col("level") != F.col("prev_level")),
                                 F.lit(1)).otherwise(F.lit(0)))
              .withColumn("grp", F.sum("is_change").over(
                  w.rowsBetween(Window.unboundedPreceding, Window.currentRow))))
    return (marked.groupBy("merchant_id", "grp")
            .agg(F.min("dt").alias("start_date"), F.max("dt").alias("max_dt"),
                 F.first("level").alias("level"))
            .withColumn("end_date",
                        F.when(F.col("max_dt") == last_day, F.lit(OPEN))
                         .otherwise(F.col("max_dt")))
            .drop("grp", "max_dt"))


def update_one_day(zip_df, snap, today, yesterday):
    """生产上每天跑的增量：闭旧链 + 开新链 + 新商户开链。"""
    open_df = zip_df.filter(F.col("end_date") == OPEN)
    changed = (open_df.alias("z").join(snap.alias("s"), "merchant_id")
               .filter(F.col("z.level") != F.col("s.level"))
               .select(F.col("z.merchant_id").alias("merchant_id")))
    new_ids = snap.select("merchant_id").join(
        open_df.select("merchant_id"), "merchant_id", "left_anti")
    closed = (zip_df.join(changed.withColumn("chg", F.lit(1)), "merchant_id", "left")
              .withColumn("end_date",
                          F.when((F.col("chg") == 1) & (F.col("end_date") == OPEN),
                                 F.lit(yesterday)).otherwise(F.col("end_date")))
              .drop("chg")
              .select("merchant_id", "level", "start_date", "end_date"))
    fresh = (snap.join(changed.union(new_ids), "merchant_id", "left_semi")
             .select("merchant_id", "level",
                     F.lit(today).alias("start_date"), F.lit(OPEN).alias("end_date")))
    return closed.union(fresh), changed.count(), new_ids.count()


t_all = time.time()
make_snapshots()

snap_all = spark.read.parquet(SNAP).localCheckpoint(eager=True)
snap_rows = snap_all.count()

t0 = time.time()
zip_rle = build_zip_rle(snap_all).localCheckpoint(eager=True)
rle_rows = zip_rle.count()
print(f"[2/4] 变更点压缩：{snap_rows:,} 行快照 -> {rle_rows:,} 行拉链"
      f"（{snap_rows/rle_rows:.1f}x），{time.time()-t0:.1f}s", flush=True)

# 前 29 天回填 + 第 30 天真实增量，用来验证每日更新逻辑
zip_29 = build_zip_rle(snap_all.filter(F.col("dt") < day_of(DAYS - 1))).localCheckpoint(eager=True)
zip_inc, n_changed, n_new = update_one_day(
    zip_29, snap_all.filter(F.col("dt") == day_of(DAYS - 1)),
    day_of(DAYS - 1), day_of(DAYS - 2))
zip_inc = zip_inc.localCheckpoint(eager=True)
print(f"[3/4] 每日增量：变更 {n_changed:,} 个商户，新增 {n_new:,} 个，"
      f"增量后 {zip_inc.count():,} 行（回填 {rle_rows:,} 行）", flush=True)

diff_inc = (zip_inc.alias("a")
            .join(zip_rle.alias("b"),
                  (F.col("a.merchant_id") == F.col("b.merchant_id")) &
                  (F.col("a.start_date") == F.col("b.start_date")), "full_outer")
            .filter(F.col("a.merchant_id").isNull() | F.col("b.merchant_id").isNull()
                    | (F.col("a.level") != F.col("b.level"))
                    | (F.col("a.end_date") != F.col("b.end_date")))
            .count())
zip_df = zip_inc
zip_df.write.mode("overwrite").parquet(f"{OUT}/dim_merchant_zip")
print(f"[4/4] 增量与回填差异 {diff_inc} 条，拉链表已写盘", flush=True)

mismatch, dup_open = {}, {}
for d in (0, 7, 14, 21, 29):
    day = day_of(d)
    back = zip_df.filter((F.col("start_date") <= day) & (F.col("end_date") >= day)) \
                 .select("merchant_id", "level")
    snap = snap_all.filter(F.col("dt") == day).select("merchant_id", "level")
    mismatch[day] = (back.alias("b").join(snap.alias("s"), "merchant_id", "full_outer")
                     .filter(F.col("b.level").isNull() | F.col("s.level").isNull()
                             | (F.col("b.level") != F.col("s.level"))).count())
    dup_open[day] = back.groupBy("merchant_id").count().filter(F.col("count") > 1).count()
    print(f"  回溯 {day}: 有效记录 {back.count():,} 条，与快照不一致 {mismatch[day]} 条，"
          f"重复 {dup_open[day]} 条", flush=True)

top = (zip_df.groupBy("merchant_id").count().filter(F.col("count") >= 3)
       .orderBy(F.col("count").desc()).limit(1).collect())
sample_chain = []
if top:
    mid = top[0]["merchant_id"]
    sample_chain = [{"merchant_id": r["merchant_id"], "level": r["level"],
                     "start_date": r["start_date"], "end_date": r["end_date"]}
                    for r in zip_df.filter(F.col("merchant_id") == mid)
                                   .orderBy("start_date").collect()]
    print(f"  示例商户 {mid} 的链：", flush=True)
    for r in sample_chain:
        print(f"    {r['start_date']} ~ {r['end_date']}  {r['level']}", flush=True)

res = {
    "env": {"spark": spark.version, "master": spark.sparkContext.master,
            "java": spark.sparkContext._jvm.System.getProperty("java.version")},
    "days": DAYS, "base_merchants": BASE_MERCHANTS, "new_per_day": NEW_PER_DAY,
    "snapshot_rows": snap_rows,
    "zip_rows": zip_df.count(),
    "compression": round(snap_rows / zip_df.count(), 2),
    "open_rows_latest": zip_df.filter(F.col("end_date") == OPEN).count(),
    "incremental_changed": n_changed, "incremental_new": n_new,
    "incremental_vs_backfill_diff": diff_inc,
    "snapshot_check_mismatch": mismatch,
    "duplicate_valid_rows": dup_open,
    "sample_chain": sample_chain,
    "seconds": round(time.time() - t_all, 1),
}
with open(os.path.join(OUT, "zip_result.json"), "w", encoding="utf-8") as f:
    json.dump(res, f, ensure_ascii=False, indent=2)
print(json.dumps(res, ensure_ascii=False, indent=2), flush=True)
spark.stop()
