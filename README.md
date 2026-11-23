# 离线数仓：ODS → DWD → DWS → ADS（Spark 单机可复现）

一套能在**一台笔记本上跑完**的离线数仓：Spark 数据生成 → 四层落盘 → 拉链表 → 调优对照 → ADS 落库。
不依赖集群、不依赖 Hive，所有数字都能用仓库里的脚本重跑出来，结果落在 `results/*.json`。

## 环境（实测，不是照抄教程）

| 项 | 值 |
|---|---|
| 系统 | Windows 11 |
| Spark | PySpark 3.5.3（local[4]，driver 6g） |
| Java | 1.8.0_202 |
| 机器 | 16 逻辑核 / 15.2 GB 内存，单机，**不是集群** |
| 存储 | C:\bigdata-lab\warehouse（Parquet，Snappy） |

> Windows 上跑 Spark 需要 `HADOOP_HOME` 指向带 `winutils.exe`/`hadoop.dll` 的目录，并把 `%HADOOP_HOME%\bin` 加进 PATH，否则会报 `NativeIO$Windows.access0`。这是本仓库踩过的第一个坑。

## 数据

`gen_data.py` 用 Spark 生成，不是真实业务数据：

- **1.5 亿行**订单明细，7 个业务字段（order_id / merchant_id / user_id / amount / status / province / dt）
- 按 `dt` 分 **30 个日期分区**写 Parquet，Snappy 压缩后落盘 **2.4 GB**
- 分布按真实长尾构造：1% 商户吃 60% 订单，其余走长尾
- 维表 `dim_merchant` 10 万商户，但**只覆盖 7.5 万**（模拟维表更新滞后 / 埋点缺失）→ 订单里约 **25%** 的 merchant_id 在维表里找不到

```bash
py -3.14 gen_data.py            # 默认 1.5 亿行，约 10 分钟
py -3.14 gen_data.py 20000000   # 先小规模试跑
```

## 四层链路

```
ODS  原样落地，按 dt 分区，不加工
 ↓   pipeline_warehouse.py
DWD  province 归一 + 过滤负金额/空主键 + 维表 left join，匹配不到的商户打标 UNKNOWN 保留
 ↓
DWS  按 dt + 商户聚合 GMV / 订单数
 ↓
ADS  日报（30 行）+ 每日 Top10 商户（300 行）→ Parquet / CSV / MySQL(JDBC)
```

实测（`results/warehouse_result.json`）：

| 层 | 行数 | 耗时 |
|---|---|---|
| ODS | 150,000,000 | 14.0 s（count） |
| DWD | 150,000,000（丢弃 0 行） | 161.0 s |
| DWS | 2,250,030 | 83.2 s |
| ADS | 日报 30 + Top10 300 | 63.2 s |

未知商户 **37,492,367 行 / 24.99%**，全部保留并打标，日报里单独统计占比。
GMV 总额 375,775,525,709.93，与倾斜实验里的前后口径一致（见下）。

## 拉链表（`zip_dim.py`）

商户等级会变，每天存一份全量快照 30 天就是 30 倍存储，所以用拉链：

```
merchant_id | level | start_date | end_date
     1001   | SMB   | 2026-09-01 | 2026-09-15
     1001   | KA    | 2026-09-16 | 9999-12-31   ← 开链 = 当前有效
```

两段实现：

1. **回填**：读 30 天快照，`lag(level)` 找变化点 → 按变化点累计编号分组 → 连续相同状态压成一个区间。30 天快照 **3,087,000 行 → 拉链 195,387 行（15.8×）**。
2. **每日增量**：取当日快照与 `end_date = 9999-12-31` 的开链记录比对，变化商户闭旧链开新链，新商户直接开链，未变化不动。用第 30 天数据真跑一次：变更 3,149 个、新增 200 个，**与全量回填结果逐条比对差异 0 条**。

历史回溯校验（抽 5 天）：与当日快照**不一致 0 条，同一天重复 0 条**。

```sql
-- 查任意一天快照
select merchant_id, level from dim_merchant_zip
where start_date <= '2026-09-15' and end_date >= '2026-09-15';
```

## 调优（严格对照，`bench_strict.py`）

早期版本 `bench3.py` 是**单次计时**，且 shuffle 对照另起了一个 SparkSession、SQL 也略有差异（少一个 count 聚合、多一个金额过滤）——那种数字拼不成一条链，被追问就崩。所以重做了 `bench_strict.py`：**同一份数据、同一个 session、同一条 SQL，每项 3 次取中位数，AQE 关闭**。

| 优化项 | 优化前 | 优化后 | 结论 |
|---|---|---|---|
| dt 等值条件（分区裁剪，30 天 → 单日） | 58.4 s | 4.7 s | 12.4×；输出同时从 300 万行降到 10 万行 |
| shuffle 分区 200 → 20（同一条 SQL，只改 conf） | 4.6 s | 1.4 s | 3.3× |
| 列裁剪（读 6 列 → 2 列） | 4.7 s | 4.7 s | 读盘 79.8 MB → 38.0 MB（47.7%），耗时无差异 |

三点必须说清，否则数字站不住：

- **首跑冷启动 88.5 s 不计入**：JVM 预热 + 磁盘缓存，三次 88.5 / 58.4 / 57.7，取中位数 58.4。
- **61.7 s → 8.3 s 里有水分**：旧口径的 61.7 → 8.3 除了分区裁剪，输出行数也从 300 万降到 10 万，工作量本身变了。
- **列裁剪是负结果**：这个规模下瓶颈在 shuffle 和聚合，不在 IO。所以不把它当提速项报，改用 Parquet 元数据证明读盘量减半（`col_size.py`）。
  面试口径：**数据小看执行计划（ReadSchema / PartitionFilters），数据大才看秒数。**

## 倾斜：证明「不需要加盐」（`bench3.py` 的 bench 部分，`results/bench_result.json`）

关 AQE、200 分区跑 `group by dt, merchant_id` 的 sum/count：

- 各分区行数 max **15,293** / median **14,999** → 比值 **1.0**，不倾斜
- 原因是聚合算子在 map 端有 combine，同一个 key 的几千万行在 shuffle 之前就被压成一条 `(sum, count)`
- 硬上加盐改造：**53.9 s → 378.2 s**（慢 7 倍），GMV 前后 **375,775,525,709.93** 一致

结论写成能力点而不是遗憾：**能证明一条链路不需要优化，比瞎优化有价值。** 会倾斜的是 join（不能预聚合）、count distinct / collect_set、窗口 partitionBy 单 key 数据量大。

## 清洗规则验证（`clean_rules_test.py`）

1.5 亿行生成数据本身干净（负金额 0、空主键 0），直接跑清洗规则命中 0 行是没法回答「洗掉了什么」的。所以单独造 5 万行脏样本验证规则：

| 规则 | 命中 |
|---|---|
| amount <= 0 过滤 | 3,000 |
| order_id 为空过滤 | 50 |
| order_id 重复去重 | 1,500 |
| province 前后空格归一 | 2,500（归一后 distinct = 1） |

输入 50,000 → 输出 45,450。

## ADS 落 MySQL

```bash
# 1) 建库建表
"C:\Program Files\MySQL\MySQL Server 26.10\bin\mysql.exe" -u root -p < sql/ads_ddl.sql
# 2) Spark JDBC 导出（读 parquet，写库，再从库里读回来校验行数与 GMV）
set MYSQL_USER=root & set MYSQL_PASSWORD=******
py -3.14 ads_to_mysql.py
```

驱动：`mysql-connector-j-8.4.0.jar`（Spark 3.5 自带的是 5.1 版驱动，连 MySQL 8+ 会踩 `caching_sha2_password`）。

实测（本机另起了一个 3307 端口的实例，不动原有的 3306）：

```bash
mysqld --initialize-insecure --datadir=C:/bigdata-lab/mysql-dev/data --console
mysqld --datadir=C:/bigdata-lab/mysql-dev/data --port=3307 --bind-address=127.0.0.1 --mysqlx=OFF --console
mysql -u root -h 127.0.0.1 -P 3307 < sql/ads_ddl.sql
```

回读校验（直接从 MySQL 查，不经过 Spark）：

| 指标 | 值 |
|---|---|
| ads_daily_summary | 30 行 |
| GMV 合计 | 375,775,525,709.93（与 Parquet 一致） |
| 订单数 | 150,000,000 |
| 未知商户订单 | 37,492,367（24.99%） |
| ads_top_merchant | 300 行（30 天 × Top10），单日第一行是 UNKNOWN 桶 31.3 亿 |

> UNKNOWN 排在每日 Top1 是正常的：它是「维表匹配不到」的汇总桶，正好说明打标后没有丢数据。

## 目录

```
gen_data.py            造 1.5 亿行数据（含长尾分布）
pipeline_warehouse.py  ODS→DWD→DWS→ADS
zip_dim.py             拉链表：变更点压缩 + 每日增量 + 回溯校验
bench_strict.py        严格调优对照（同 session / 同 SQL / 3 次中位数）
bench3.py              早期单次计时版本（保留作对照，说明为什么重测）
col_size.py            Parquet 按列统计压缩体积（列裁剪读盘量）
clean_rules_test.py    清洗规则命中验证
ads_to_mysql.py        ADS 落 MySQL（JDBC）
demo_share.py          小数据演示：只看执行计划不看秒数
sql/ads_ddl.sql        ADS 表结构
results/*.json         所有实测结果
```

## 已知不足

- 单机 local[4]，没碰过真集群的 YARN 资源队列、数据本地性、猜测执行
- 没有调度系统，四层是脚本串的；生产上要 Airflow / DolphinScheduler
- 没做过实时链路（Flink）
- 维表覆盖率 75% 是刻意构造的，真实场景里这类缺失来自上游埋点
