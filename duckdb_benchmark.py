#!/usr/bin/python3

import duckdb
import os
import sys

input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
sort_col = sys.argv[3]

data_dir_path = "."

duckdb_file_path = os.path.join(data_dir_path, "data.duckdb")
with duckdb.connect(database=duckdb_file_path, read_only=False) as conn:
    conn.execute(
        f"""COPY (SELECT * FROM read_parquet('{input_file_path}')
        ORDER BY {sort_col} ASC)
        TO '{output_file_path}' (FORMAT PARQUET)"""
    )
