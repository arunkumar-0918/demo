import time
import pandas as pd
import os
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class TimeTaken:
    def __init__(self):
        self.log_file = "time_taken_log.csv"
        self.create_csv()
    
    def create_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.log_file):
            df = pd.DataFrame(columns=["Operation", "Time_Taken_Seconds"])
            df.to_csv(self.log_file, index=False)
    
    def log_time(self, operation, time_taken):
        """Log operation time to CSV file"""
        with open(self.log_file, "a") as f:
            f.write(f"{operation},{time_taken:.4f}\n")
        print(f"{operation}: {time_taken:.4f} seconds")
    
    def start(self):
        self.start_time = time.time()
        return self.start_time
    
    def end_time(self):
        self.end_time_val = time.time()
        return self.end_time_val
    
    def calculate(self, start, end, operation_name):
        time_taken = end - start
        self.log_time(operation_name, time_taken)
        return time_taken


class PandasMerge:
    def __init__(self, timer):
        self.timer = timer
    
    def merge(self, df1, df2, operation_name="Pandas Merge"):
        start = self.timer.start()
        result = pd.merge(df1, df2, on="id", how="left")
        end = self.timer.end_time()
        self.timer.calculate(start, end, operation_name)
        return result


class IcebergWriter:
    def __init__(self, spark, timer=None):
        self.spark = spark
        self.timer = timer
    
    def create_table(self, df, table_name):
        """Create initial Iceberg table"""
        start = self.timer.start() if self.timer else None
        df.writeTo(f"iceberg.analytics_db.{table_name}").create()
        if self.timer and start:
            end = self.timer.end_time()
            self.timer.calculate(start, end, f"Create Table: {table_name}")
    
    def append_table(self, df, table_name):
        """Append to existing Iceberg table"""
        start = self.timer.start() if self.timer else None
        df.writeTo(f"iceberg.analytics_db.{table_name}").append()
        if self.timer and start:
            end = self.timer.end_time()
            self.timer.calculate(start, end, f"Append Table: {table_name}")


class IcebergOperations(ABC):
    @abstractmethod
    def merge_table(self, df, table):
        pass


class IcebergMerger(IcebergOperations):
    def __init__(self, spark, timer=None):
        self.spark = spark
        self.timer = timer
    
    def merge_table(self, df, table):
        """Merge/upsert data into Iceberg table"""
        start = self.timer.start() if self.timer else None
        df.createOrReplaceTempView("updates")
        self.spark.sql(f"""
            MERGE INTO iceberg.analytics_db.{table} t
            USING updates s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        if self.timer and start:
            end = self.timer.end_time()
            self.timer.calculate(start, end, f"Merge Into: {table}")
        print(f"Merged data into {table}")


def setup_spark(warehouse_path):
    """Initialize Spark with Iceberg configuration"""
    # Create warehouse directory if it doesn't exist
    os.makedirs(warehouse_path, exist_ok=True)
    
    spark = (
        SparkSession.builder
        .appName("Iceberg Local Demo")
        .master("local[*]")  # Added explicit local mode
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", 
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse_path)
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def main():
    # Configuration
    warehouse_path = os.path.join(os.getcwd(), "iceberg_warehouse")
    source_file = "demo.csv"
    update_file = "update.csv"
    
    # Initialize Spark
    print("Initializing Spark with Iceberg...")
    spark = setup_spark(warehouse_path)
    
    # Verify catalogs
    print("\nAvailable Catalogs:")
    spark.sql("SHOW CATALOGS").show()
    
    # Create database if it doesn't exist
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.analytics_db")
    print("\nCreated/verified analytics_db database")
    
    # ========================================
    # PANDAS MERGE TIMING
    # ========================================
    print("\n" + "="*50)
    print("PANDAS MERGE TIMING")
    print("="*50)
    
    timer = TimeTaken()
    pandas_merger = PandasMerge(timer)
    
    df1 = pd.read_csv(os.path.join(os.getcwd(), source_file))
    df2 = pd.read_csv(os.path.join(os.getcwd(), update_file))
    
    pandas_merger.merge(df1, df2, "Pandas Merge Operation")
    
    # ========================================
    # SPARK ICEBERG OPERATIONS
    # ========================================
    print("\n" + "="*50)
    print("SPARK ICEBERG OPERATIONS")
    print("="*50)
    
    # Read CSV files into Spark DataFrames
    spark_df1 = spark.read.csv(
        os.path.join(os.getcwd(), source_file),
        header=True,
        inferSchema=True
    )
    
    spark_df2 = spark.read.csv(
        os.path.join(os.getcwd(), update_file),
        header=True,
        inferSchema=True
    )
    
    print("\nSource data schema:")
    spark_df1.printSchema()
    print(f"Source records: {spark_df1.count()}")
    
    print("\nUpdate data schema:")
    spark_df2.printSchema()
    print(f"Update records: {spark_df2.count()}")
    
    # Initialize writer and merger with timer
    writer = IcebergWriter(spark, timer)
    merger = IcebergMerger(spark, timer)
    
    # ========================================
    # COPY-ON-WRITE TABLE
    # ========================================
    print("\n" + "-"*50)
    print("Creating COPY-ON-WRITE table: customer_cow")
    print("-"*50)
    
    # Drop table if exists (for clean demo)
    spark.sql("DROP TABLE IF EXISTS iceberg.analytics_db.customer_cow")
    
    # Create and populate COW table
    writer.create_table(spark_df1, "customer_cow")
    print("✓ Created customer_cow table")
    
    # Show initial data
    print("\nInitial data in customer_cow:")
    spark.sql("SELECT * FROM iceberg.analytics_db.customer_cow").show()
    
    # Merge updates
    print("\nMerging updates into customer_cow...")
    merger.merge_table(spark_df2, "customer_cow")
    
    # Show merged data
    print("\nData after merge in customer_cow:")
    spark.sql("SELECT * FROM iceberg.analytics_db.customer_cow").show()
    
    # ========================================
    # MERGE-ON-READ TABLE
    # ========================================
    print("\n" + "-"*50)
    print("Creating MERGE-ON-READ table: customer_mor")
    print("-"*50)
    
    # Drop table if exists
    spark.sql("DROP TABLE IF EXISTS iceberg.analytics_db.customer_mor")
    
    # Create MOR table with table property
    start = timer.start()
    spark_df1.writeTo("iceberg.analytics_db.customer_mor") \
        .tableProperty("write.delete.mode", "merge-on-read") \
        .tableProperty("write.update.mode", "merge-on-read") \
        .tableProperty("write.merge.mode", "merge-on-read") \
        .create()
    end = timer.end_time()
    timer.calculate(start, end, "Create Table: customer_mor")
    print("✓ Created customer_mor table")
    
    # Show initial data
    print("\nInitial data in customer_mor:")
    spark.sql("SELECT * FROM iceberg.analytics_db.customer_mor").show()
    
    # Merge updates
    print("\nMerging updates into customer_mor...")
    merger.merge_table(spark_df2, "customer_mor")
    
    # Show merged data
    print("\nData after merge in customer_mor:")
    spark.sql("SELECT * FROM iceberg.analytics_db.customer_mor").show()
    
    # ========================================
    # METADATA INSPECTION
    # ========================================
    print("\n" + "="*50)
    print("METADATA INSPECTION")
    print("="*50)
    
    print("\nCOW Table Files:")
    spark.sql("SELECT file_path, file_format, record_count FROM iceberg.analytics_db.customer_cow.files").show(truncate=False)
    
    print("\nCOW Table Snapshots:")
    spark.sql("SELECT * FROM iceberg.analytics_db.customer_cow.snapshots").show(truncate=False)
    
    print("\nMOR Table Files:")
    spark.sql("SELECT file_path, file_format, record_count FROM iceberg.analytics_db.customer_mor.files").show(truncate=False)
    
    print("\nMOR Table Snapshots:")
    spark.sql("SELECT * FROM iceberg.analytics_db.customer_mor.snapshots").show(truncate=False)
    
    # ========================================
    # DISPLAY TIME LOG
    # ========================================
    print("\n" + "="*50)
    print("PERFORMANCE SUMMARY")
    print("="*50)
    
    if os.path.exists("time_taken_log.csv"):
        time_log = pd.read_csv("time_taken_log.csv")
        print("\nAll Operations Timing:")
        print(time_log.to_string(index=False))
        print(f"\nTotal Time: {time_log['Time_Taken_Seconds'].sum():.4f} seconds")
    
    print("\n✓ Demo completed successfully!")
    print(f"✓ Timing log saved to: time_taken_log.csv")
    
    # Cleanup
    spark.stop()


if __name__ == "__main__":
    main()