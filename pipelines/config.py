"""
pipelines/config.py
---------------------
Central, no-hardcoded-paths configuration shared by every pipeline stage.
All paths are derived from the project root so the project can be moved
or cloned anywhere without editing code.
"""
import os
import platform
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PATHS = {
    "raw_root": os.path.join(PROJECT_ROOT, "data", "raw"),
    "bronze_root": os.path.join(PROJECT_ROOT, "data", "bronze"),
    "silver_root": os.path.join(PROJECT_ROOT, "data", "silver"),
    "gold_root": os.path.join(PROJECT_ROOT, "data", "gold"),
    "dashboard_root": os.path.join(PROJECT_ROOT, "data", "dashboard"),
    "logs_root": os.path.join(PROJECT_ROOT, "logs"),
}

POSTGRES = {
    "host": os.environ.get("UMIP_PG_HOST", "localhost"),
    "port": os.environ.get("UMIP_PG_PORT", "5432"),
    "dbname": os.environ.get("UMIP_PG_DB", "umip_platform"),
    "user": os.environ.get("UMIP_PG_USER", "umip_admin"),
    "password": os.environ.get("UMIP_PG_PASSWORD", "umip_password"),
}

def get_spark(app_name):
    import os
    import sys

    # Hadoop
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    # Force Spark to use this virtual environment's Python
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", os.path.join(PROJECT_ROOT, "spark-warehouse"))
        .config("spark.hadoop.hadoop.home.dir", r"C:\hadoop")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
def psycopg2_connect():
    """Plain psycopg2 connection, used for metadata/audit/error logging and
    for pulling Postgres source tables into Spark."""
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES["host"], port=POSTGRES["port"], dbname=POSTGRES["dbname"],
        user=POSTGRES["user"], password=POSTGRES["password"],
    )


def read_postgres_table(spark, table_name, columns="*"):
    """
    Reads a Postgres table into a Spark DataFrame without needing a JDBC jar:
    psycopg2 -> pandas -> spark.createDataFrame(). Fine for metadata-sized
    tables (thousands-millions of rows); for true big-data Postgres sources
    at Uber's actual scale, you'd swap this for the JDBC connector instead.
    """
    import pandas as pd
    conn = psycopg2_connect()
    try:
        pdf = pd.read_sql(f"SELECT {columns} FROM {table_name}", conn)
    finally:
        conn.close()
    return spark.createDataFrame(pdf)
