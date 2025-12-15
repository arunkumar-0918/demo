from pyspark.sql import SparkSession


sc = (
    SparkSession.builder
    .appName("demo")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config(
        "spark.sql.catalog.iceberg.warehouse",
        "file:///Users/arunkumar/Desktop/icebergtable/iceberg"
    )
    .getOrCreate()
)

sc.sql("show catalogs").show()

db_nm = "analytics_db"
file_dir = "/tmp/warehouse"
db_dir = "analytics"
tablename = "customer"

# -------------------------------
# COPY-ON-WRITE TABLE
# -------------------------------
sc.sql("""
CREATE DATABASE IF NOT EXISTS iceberg.analytics_db
""")


sc.sql(f"""
CREATE TABLE IF NOT EXISTS iceberg.{db_nm}.customer_cow (
    id BIGINT,
    name STRING,
    email STRING,
    city STRING,
    events STRING,
    event_datetime TIMESTAMP
)
using iceberg
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write',
    'format-version'    = '2'
)
""")

# -------------------------------
# MERGE-ON-READ TABLE
# -------------------------------
sc.sql(f"""
CREATE TABLE IF NOT EXISTS iceberg.{db_nm}.customer_mor (
    id BIGINT,
    name STRING,
    email STRING,
    city STRING,
    events STRING,
    event_datetime TIMESTAMP
)
using iceberg
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read',
    'format-version'    = '2'
)

""")

# -------------------------------
# ALTER MOR → COW
# -------------------------------
'''sc.sql(f"""
ALTER TABLE {file_dir}/{db_dir}/{tablename}_mor
SET TBLPROPERTIES (
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write'
)
""")'''

# -------------------------------
# ALTER COW → MOR
# -------------------------------
'''sc.sql(f"""
ALTER TABLE {file_dir}/{db_dir}/{tablename}_cow
SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
)
""")'''

sc.sql("show tables").show()