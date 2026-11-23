-- ADS 落库 DDL（MySQL）：Spark JDBC 用 overwrite + truncate，表要先存在
CREATE DATABASE IF NOT EXISTS warehouse DEFAULT CHARACTER SET utf8mb4;
USE warehouse;

CREATE TABLE IF NOT EXISTS ads_daily_summary (
  dt                VARCHAR(10)   NOT NULL PRIMARY KEY,
  gmv               DECIMAL(20,2) NULL,
  order_cnt         BIGINT        NULL,
  unknown_order_cnt BIGINT        NULL
);

CREATE TABLE IF NOT EXISTS ads_top_merchant (
  dt           VARCHAR(10)   NOT NULL,
  merchant_key VARCHAR(16)   NOT NULL,
  gmv          DECIMAL(20,2) NULL,
  order_cnt    BIGINT        NULL,
  rn           INT           NOT NULL,
  PRIMARY KEY (dt, rn)
);
