from loguru import logger
from pyspark.sql import SparkSession

from benchmarking.benchmarks.base import Benchmark
from benchmarking.settings import BenchmarkSettings
from benchmarking.utils import measure_execution_time


class DeltaTimeTravelBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Delta] Running time travel benchmark")
        delta_path = f"{self.settings.data_path}/{self.settings.delta_table}"
        self.query_specific_version(spark, delta_path, 0)
        self.query_specific_version(spark, delta_path, 1)
        self.query_specific_version(spark, delta_path, 2)
        self.query_specific_version(spark, delta_path, 3)

    @measure_execution_time
    def query_specific_version(self, spark: SparkSession, path: str, version: int):
        df = spark.read.format("delta").option("versionAsOf", version).load(path)
        count = df.count()
        logger.info(f"Version {version} has {count} rows.")


class IcebergTimeTravelBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Iceberg] Running time travel benchmark")

        snapshot_ids = self.get_snapshot_id(spark, self.settings.iceberg_table)
        for snapshot_id in snapshot_ids:
            self.query_snapshot(spark, self.settings.iceberg_table, snapshot_id)

        # Query the latest snapshot
        self.query_snapshot(spark, self.settings.iceberg_table)

    @measure_execution_time
    def query_snapshot(self, spark: SparkSession, table, snapshot_id=None):
        if snapshot_id is not None:
            df = (
                spark.read.format("iceberg")
                .option("snapshot-id", snapshot_id)
                .load(table)
            )
        else:
            df = spark.read.format("iceberg").load(table)

        count = df.count()
        logger.info(
            f"Snapshot {snapshot_id if snapshot_id else 'latest'} has {count} rows."
        )

    def get_snapshot_id(self, spark, table):
        snapshots = spark.sql(f"SELECT snapshot_id FROM default.{table}.snapshots")
        return [row.snapshot_id for row in snapshots.collect()]


class HudiTimeTravelBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Hudi] Running time travel benchmark")

        # Get commit times after modifications
        commit_times = self.get_commit_times(spark, self.settings.hudi_table)

        # Query each commit by time to benchmark time travel
        for commit_time in commit_times:
            self.query_commit(spark, self.settings.hudi_table, commit_times)

        # Query the latest state
        self.query_commit(spark, self.settings.hudi_table)

    @measure_execution_time
    def query_commit(self, spark, base_path, commit_time=None):
        if commit_time:
            df = (
                spark.read.format("hudi")
                .option("as.of.instant", commit_time)
                .load("s3a://data/" + base_path + "/*")
            )
        else:
            df = spark.read.format("hudi").load("s3a://data/" + base_path + "/*")

        count = df.count()
        logger.info(
            f"Commit {commit_time if commit_time else 'latest'} has {count} rows."
        )

    def get_commit_times(self, spark, base_path):
        df = spark.read.format("hudi").load("s3a://data/" + base_path + "/*")
        commitTimesDf = (
            df.select("_hoodie_commit_time")
            .distinct()
            .orderBy("_hoodie_commit_time", ascending=False)
        )

        commitTimes = [row["_hoodie_commit_time"] for row in commitTimesDf.collect()]

        return commitTimes


class TimeTravelBenchmarkRunner(Benchmark):
    def __init__(self, format: str, benchmark_settings: BenchmarkSettings) -> None:
        self.benchmarks = {
            "delta": DeltaTimeTravelBenchmark,
            "iceberg": IcebergTimeTravelBenchmark,
            "hudi": HudiTimeTravelBenchmark,
        }
        self.benchmark = self.benchmarks[format](benchmark_settings)  # type: ignore

    def start(self, spark: SparkSession) -> None:
        self.benchmark.start(spark)
