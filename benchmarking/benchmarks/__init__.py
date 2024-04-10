from loguru import logger

from benchmarking.benchmarks.base import Benchmark
from benchmarking.benchmarks.ingestion import IngestionBenchmarkRunner
from benchmarking.benchmarks.initial_ingestion import (
    InitialIngestionBenchmarkRunner,
)
from benchmarking.benchmarks.query_efficiency import QueryEfficiencyBenchmarkRunner
from benchmarking.benchmarks.time_travel import TimeTravelBenchmarkRunner
from benchmarking.settings import BenchmarkSettings


class InvalidBenchmarkTypeException(Exception):
    """Exception raised when an invalid benchmark type is passed."""

    def __init__(self, benchmark_type, message="Invalid benchmark type"):
        self.benchmark_type = benchmark_type
        self.message = f"{message}: '{benchmark_type}'"
        super().__init__(self.message)


def get_benchmark_runner(
    benchmark_type: str, format: str, benchmark_settings: BenchmarkSettings
) -> Benchmark:
    match benchmark_type:
        case "time-travel":
            logger.info("Initializing time travel benchmark")
            return TimeTravelBenchmarkRunner(
                format=format, benchmark_settings=benchmark_settings
            )
        case "ingestion":
            logger.info("Initializing ingestion benchmark")
            return IngestionBenchmarkRunner(
                format=format, benchmark_settings=benchmark_settings
            )
        case "initial-ingestion":
            logger.info("Initializing initial ingestion benchmark")
            return InitialIngestionBenchmarkRunner(
                format=format, benchmark_settings=benchmark_settings
            )
        case "query-efficiency":
            logger.info("Initializing query efficiency benchmark")
            return QueryEfficiencyBenchmarkRunner(
                format=format, benchmark_settings=benchmark_settings
            )
        case _:
            logger.warning("Please specify a valid benchmark to run.")
            raise InvalidBenchmarkTypeException(benchmark_type)
