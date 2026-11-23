"""列裁剪到底省多少 IO：直接读 Parquet 文件元数据，按列统计压缩后体积。

Spark 的 scan 指标在本地模式下不容易取，PyArrow 读 footer 反而更直接：
统计单日分区里每个列 chunk 的 total_compressed_size，就能算出
「只读 merchant_id + amount」相对「读全部 6 列」省掉多少读盘量。

用法：py -3.14 col_size.py [分区目录]
"""
import glob
import json
import os
import sys

import pyarrow.parquet as pq

day_dir = sys.argv[1] if len(sys.argv) > 1 else r"C:\bigdata-lab\warehouse\ods_order\dt=2026-09-15"
files = glob.glob(os.path.join(day_dir, "*.parquet"))
col, total, rows = {}, 0, 0
for f in files:
    md = pq.ParquetFile(f).metadata
    rows += md.num_rows
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            cc = md.row_group(rg).column(c)
            name = cc.path_in_schema.split(".")[0]
            col[name] = col.get(name, 0) + cc.total_compressed_size

need = col.get("merchant_id", 0) + col.get("amount", 0)
total = sum(col.values())
res = {
    "partition": day_dir,
    "files": len(files),
    "rows": rows,
    "column_bytes": {k: v for k, v in sorted(col.items(), key=lambda x: -x[1])},
    "all_columns_mb": round(total / 1024 / 1024, 2),
    "needed_columns_mb": round(need / 1024 / 1024, 2),
    "read_ratio": round(need / total, 3),
}
print(json.dumps(res, ensure_ascii=False, indent=2))
with open(os.path.join(os.path.dirname(day_dir), "..", "col_size_result.json"), "w", encoding="utf-8") as fh:
    json.dump(res, fh, ensure_ascii=False, indent=2)
