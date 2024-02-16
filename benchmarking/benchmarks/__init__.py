from loguru import logger

from benchmarking.benchmarks.base import Benchmark
from benchmarking.benchmarks.ingestion import IngestionBenchmark
from benchmarking.benchmarks.init_ingestion import (
    InitIngestionBenchmarkRunner,
)
from benchmarking.benchmarks.time_travel import TimeTravelBenchmark


class InvalidBenchmarkTypeException(Exception):
    """Exception raised when an invalid benchmark type is passed."""

    def __init__(self, benchmark_type, message="Invalid benchmark type"):
        self.benchmark_type = benchmark_type
        self.message = f"{message}: '{benchmark_type}'"
        super().__init__(self.message)


def get_benchmark_runner(benchmark_type: str) -> Benchmark:
    match benchmark_type:
        case "time-travel":
            logger.info("Initializing time travel benchmark")
            return TimeTravelBenchmark()
        case "ingestion":
            logger.info("Initializing ingestion benchmark")
            return IngestionBenchmark()
        case "init-ingestion":
            return InitIngestionBenchmarkRunner()
        case _:
            logger.warning("Please specify a valid benchmark to run.")
            raise InvalidBenchmarkTypeException(benchmark_type)
