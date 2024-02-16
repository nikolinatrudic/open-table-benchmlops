import time

import pandas as pd
from loguru import logger
from pyspark.sql import SparkSession

from benchmarking.benchmarks.base import Benchmark
from benchmarking.utils import download_dataset, get_project_root

DATASET_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
DATASET_NAME = "Online Retail.xlsx"
DATASET_DESTINATION_PATH = get_project_root() / "data" / DATASET_NAME


class DeltaInitIngestionBenchmark(Benchmark):
    def start(self, spark: SparkSession):
        logger.info("[Delta] Running init ingestion benchmark")

        df = pd.read_excel(str(DATASET_DESTINATION_PATH))
        spark_df = spark.createDataFrame(df)

        start_time = time.time()

        print(spark_df.count())
        # spark_df.write.format("delta").mode("overwrite").save(
        #     str(get_project_root() / "data" / "delta")
        # )

        end_time = time.time()
        duration = end_time - start_time

        logger.info(f"[Delta] Init ingestion benchmark completed in {duration} seconds")


class IcebergInitIngestionBenchmark(Benchmark):
    def start(self, spark: SparkSession):
        logger.info("[Iceberg] Running init ingestion benchmark")

        df = pd.read_excel(str(DATASET_DESTINATION_PATH))
        spark_df = spark.createDataFrame(df)

        start_time = time.time()

        print(spark_df.count())
        end_time = time.time()
        duration = end_time - start_time

        logger.info(
            f"[Iceberg] Init ingestion benchmark completed in {duration} seconds"
        )


class HudiInitIngestionBenchmark(Benchmark):
    def start(self, spark: SparkSession):
        logger.info("[Hudi] Running init ingestion benchmark")

        df = pd.read_excel(str(DATASET_DESTINATION_PATH))
        spark_df = spark.createDataFrame(df)

        start_time = time.time()

        print(spark_df.count())
        end_time = time.time()
        duration = end_time - start_time

        logger.info(f"[Hudi] Init ingestion benchmark completed in {duration} seconds")


class InitIngestionBenchmarkRunner(Benchmark):
    def __init__(self) -> None:
        download_dataset(
            dataset_url=DATASET_URL, destination_path=str(DATASET_DESTINATION_PATH)
        )
        self.benchmarks = [
            DeltaInitIngestionBenchmark(),
            IcebergInitIngestionBenchmark(),
            HudiInitIngestionBenchmark(),
        ]

    def start(self, spark: SparkSession) -> None:
        for benchmark in self.benchmarks:
            benchmark.start(spark)
