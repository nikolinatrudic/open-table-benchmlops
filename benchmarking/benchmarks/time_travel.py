from loguru import logger
from pyspark.sql import SparkSession

from benchmarking.benchmarks.base import Benchmark


class TimeTravelBenchmark(Benchmark):
    def __init__(self) -> None:
        pass

    def start(self, spark: SparkSession):
        logger.info("Running time travel benchmark")
