#!/usr/bin/python3

import duckdb
import os
import sys

duckdb_file_path = os.path.join(".", "tpch_duckdb.duckdb")
with duckdb.connect(database=duckdb_file_path, read_only=False) as conn:
    conn.execute(
        f"""
        INSTALL tpch;
        LOAD tpch;
        DROP TABLE IF EXISTS customer;
        DROP TABLE IF EXISTS lineitem;
        DROP TABLE IF EXISTS nation;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS part;
        DROP TABLE IF EXISTS partsupp;
        DROP TABLE IF EXISTS region;
        DROP TABLE IF EXISTS supplier;
        
        CALL dbgen(sf = 10);

        COPY (SELECT * FROM lineitem) TO 'lineitem.parquet' (FORMAT 'parquet');
        """
    )

os.unlink(duckdb_file_path)
