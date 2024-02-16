from loguru import logger
from pyspark.sql import SparkSession

from benchmarking.benchmarks.base import Benchmark


class IngestionBenchmark(Benchmark):
    def __init__(self) -> None:
        pass

    def start(self, spark: SparkSession):
        logger.info("Running ingestion benchmark")
