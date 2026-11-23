"""ADS 结果写入 MySQL（Spark JDBC），供 Web/报表查询。

用法：
    set MYSQL_USER=root
    set MYSQL_PASSWORD=******
    py -3.14 ads_to_mysql.py

前提：库里已建 warehouse 库（本脚本用 mysql 客户端建库建表，调用见 README）。
"""
import os
import sys

from pyspark.sql import SparkSession

OUT = r"C:\bigdata-lab\warehouse"
JAR = r"C:\bigdata-lab\jars\mysql-connector-j-8.4.0.jar"

user = os.environ.get("MYSQL_USER", "root")
pwd = os.environ.get("MYSQL_PASSWORD", "")
url = os.environ.get("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/warehouse?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai")

if not pwd:
    sys.exit("请先设置 MYSQL_PASSWORD 环境变量")

spark = (
    SparkSession.builder.appName("ads_to_mysql")
    .master("local[2]")
    .config("spark.jars", JAR)
    .config("spark.driver.extraClassPath", JAR)
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

daily = spark.read.parquet(f"{OUT}/ads_daily_summary").orderBy("dt")
top = spark.read.parquet(f"{OUT}/ads_top_merchant").orderBy("dt", "rn")

props = {"user": user, "password": pwd, "driver": "com.mysql.cj.jdbc.Driver"}

daily.write.mode("overwrite").option("truncate", "true").jdbc(url, "ads_daily_summary", properties=props)
print("ads_daily_summary 写入", daily.count(), "行")
top.write.mode("overwrite").option("truncate", "true").jdbc(url, "ads_top_merchant", properties=props)
print("ads_top_merchant 写入", top.count(), "行")

# 回读校验：直接从 MySQL 读回来，确认下游拿到的行数和聚合值一致
back = spark.read.jdbc(url, "ads_daily_summary", properties=props)
print("回读 ads_daily_summary：", back.count(), "行，GMV 合计",
      back.agg({"gmv": "sum"}).collect()[0][0])
back.orderBy("dt").show(5, truncate=False)
spark.stop()
