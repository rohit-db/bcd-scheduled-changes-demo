"""Run a SQL file (or -c statement) against Lakebase with psycopg — used instead of psql.

Usage: run_sql.py --host H --user U --password P (-f file.sql | -c "sql")
"""
import argparse

import psycopg

p = argparse.ArgumentParser()
p.add_argument("--host", required=True)
p.add_argument("--user", required=True)
p.add_argument("--password", required=True)
p.add_argument("--dbname", default="databricks_postgres")
g = p.add_mutually_exclusive_group(required=True)
g.add_argument("-f", dest="file")
g.add_argument("-c", dest="sql")
a = p.parse_args()

sql = open(a.file).read() if a.file else a.sql
with psycopg.connect(host=a.host, port=5432, dbname=a.dbname, user=a.user, password=a.password, sslmode="require",
                     autocommit=True) as conn:
    cur = conn.execute(sql)
    while True:
        if cur.description:
            for row in cur.fetchall():
                print(*row, sep=" | ")
        if not cur.nextset():
            break
