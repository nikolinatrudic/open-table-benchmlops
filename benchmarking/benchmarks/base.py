import abc

from pyspark.sql import SparkSession


class Benchmark(abc.ABC):
    @abc.abstractmethod
    def start(self, spark: SparkSession) -> None:
        """"""
