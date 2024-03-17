from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from benchmarking.benchmarks.base import Benchmark
from benchmarking.settings import BenchmarkSettings

schema = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", FloatType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True),
    ]
)


class DeltaInitialIngestionBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Delta] Running init ingestion benchmark")

        df = spark.read.csv(self.settings.csv_dataset_path, header=True, schema=schema)
        df = df.withColumn("Date", to_date(col("InvoiceDate")))
        df.write.format("delta").partitionBy(self.settings.partition_column).mode(
            "overwrite"
        ).save(f"{self.settings.data_path}/{self.settings.delta_table}")


class IcebergInitialIngestionBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Iceberg] Running init ingestion benchmark")

        df = spark.read.csv(self.settings.csv_dataset_path, header=True, schema=schema)
        df = df.withColumn("Date", to_date(col("InvoiceDate")))

        sql_fields = self.generate_sql_fields(df)
        create_table_command = f"""
            CREATE TABLE {self.settings.iceberg_table} (
                {sql_fields}
            ) USING iceberg
            PARTITIONED BY ({self.settings.partition_column});
        """

        spark.sql(create_table_command)
        df.writeTo(self.settings.iceberg_table).overwritePartitions()

    def generate_sql_fields(self, df: DataFrame) -> str:
        schema = df.schema

        fields = []
        for field in schema.fields:
            field_sql = f"  {field.name} {field.dataType.simpleString()}"
            fields.append(field_sql)

        return ",\n  ".join(fields)


class HudiInitialIngestionBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Hudi] Running init ingestion benchmark")

        df = spark.read.csv(self.settings.csv_dataset_path, header=True, schema=schema)
        df = df.withColumn("Date", to_date(col("InvoiceDate")))

        hudi_options = {
            "hoodie.table.name": self.settings.hudi_table,
            "hoodie.datasource.write.partitionpath.field": self.settings.partition_column,
        }

        df.write.format("hudi").options(**hudi_options).mode("append").save(
            f"{self.settings.data_path}/{self.settings.hudi_table}"
        )


class InitialIngestionBenchmarkRunner(Benchmark):
    def __init__(self, format: str, benchmark_settings: BenchmarkSettings) -> None:
        self.benchmarks = {
            "delta": DeltaInitialIngestionBenchmark,
            "iceberg": IcebergInitialIngestionBenchmark,
            "hudi": HudiInitialIngestionBenchmark,
        }
        self.benchmark = self.benchmarks[format](benchmark_settings)  # type: ignore

    def start(self, spark: SparkSession) -> None:
        self.benchmark.start(spark)
