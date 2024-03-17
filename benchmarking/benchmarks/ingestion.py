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


class DeltaIngestionBenchmark:
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession, fake_data: DataFrame):
        logger.info("[Delta] Running fake transaction data ingestion benchmark")

        fake_data.write.format("delta").partitionBy(
            self.settings.partition_column
        ).mode("append").save(f"{self.settings.data_path}/{self.settings.delta_table}")


class IcebergIngestionBenchmark:
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession, fake_data: DataFrame):
        logger.info("[Iceberg] Running init ingestion benchmark")

        fake_data.write.format("iceberg").mode("append").save(
            self.settings.iceberg_table
        )


class HudiIngestionBenchmark:
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession, fake_data: DataFrame):
        logger.info("[Hudi] Running init ingestion benchmark")


class IngestionBenchmarkRunner(Benchmark):
    def __init__(self, format: str, benchmark_settings: BenchmarkSettings) -> None:
        self.benchmarks = {
            "delta": DeltaIngestionBenchmark,
            "iceberg": IcebergIngestionBenchmark,
            "hudi": HudiIngestionBenchmark,
        }
        self.benchmark = self.benchmarks[format](benchmark_settings)
        self.settings = benchmark_settings
        self.schema = StructType(
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

    def start(self, spark: SparkSession) -> None:
        for i in range(3):
            fake_data = spark.read.csv(
                f"{self.settings.data_path}/raw/FakeTransactionData{i}.csv",
                header=True,
                schema=self.schema,
            )
            fake_data = fake_data.withColumn("Date", to_date(col("InvoiceDate")))
            self.benchmark.start(spark, fake_data)  # type: ignore
