from loguru import logger
from pyspark.sql import SparkSession

from benchmarking.benchmarks.base import Benchmark
from benchmarking.benchmarks.queries import eda, feature_engineering, training
from benchmarking.settings import BenchmarkSettings
from benchmarking.utils import measure_execution_time


class DeltaQueryEfficiencyBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Delta] Running query efficiency benchmark")
        df = spark.read.format("delta").load(
            f"{self.settings.data_path}/{self.settings.delta_table}"
        )

        logger.info("[Delta] Running EDA queries")
        eda_queries = eda.get_eda_queries(df)
        for query_name, res_df in eda_queries.items():
            logger.info(f"[Delta] Running {query_name} query")
            self.run_query(res_df)

        logger.info("[Delta] Running Feature Engineering queries")
        fe_queries = feature_engineering.get_feature_engineering_queries(df)
        for query_name, res_df in fe_queries.items():
            logger.info(f"[Delta] Running {query_name} query")
            self.run_query(res_df)

        logger.info("[Delta] Running Model Training queries")
        training_queries = training.get_model_training_queries(df)
        for query_name, res_df in training_queries.items():
            logger.info(f"[Delta] Running {query_name} query")
            self.run_query(res_df)

    @measure_execution_time
    def run_query(self, df):
        count = df.count()
        logger.info(f"Query returned {count} rows.")


class IcebergQueryEfficiencyBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Iceberg] Running query efficiency benchmark")
        df = spark.read.format("iceberg").load(self.settings.iceberg_table)

        logger.info("[Iceberg] Running EDA queries")
        eda_queries = eda.get_eda_queries(df)
        for query_name, res_df in eda_queries.items():
            logger.info(f"[Iceberg] Running {query_name} query")
            self.run_query(res_df)

        logger.info("[Iceberg] Running Feature Engineering queries")
        fe_queries = feature_engineering.get_feature_engineering_queries(df)
        for query_name, res_df in fe_queries.items():
            logger.info(f"[Iceberg] Running {query_name} query")
            self.run_query(res_df)

        logger.info("[Iceberg] Running Model Training queries")
        training_queries = training.get_model_training_queries(df)
        for query_name, res_df in training_queries.items():
            logger.info(f"[Iceberg] Running {query_name} query")
            self.run_query(res_df)

    @measure_execution_time
    def run_query(self, df):
        count = df.count()
        logger.info(f"Query returned {count} rows.")


class HudiQueryEfficiencyBenchmark(Benchmark):
    def __init__(self, benchmark_settings: BenchmarkSettings) -> None:
        self.settings = benchmark_settings

    def start(self, spark: SparkSession):
        logger.info("[Hudi] Running query efficiency benchmark")
        df = spark.read.format("hudi").load(
            "s3a://data/" + self.settings.hudi_table + "/*"
        )

        logger.info("[Hudi] Running EDA queries")
        eda_queries = eda.get_eda_queries(df)
        for query_name, res_df in eda_queries.items():
            logger.info(f"[Hudi] Running {query_name} query")
            self.run_query(res_df)

        logger.info("[Hudi] Running Feature Engineering queries")
        fe_queries = feature_engineering.get_feature_engineering_queries(df)
        for query_name, res_df in fe_queries.items():
            logger.info(f"[Hudi] Running {query_name} query")
            self.run_query(res_df)

        logger.info("[Hudi] Running Model Training queries")
        training_queries = training.get_model_training_queries(df)
        for query_name, res_df in training_queries.items():
            logger.info(f"[Hudi] Running {query_name} query")
            self.run_query(res_df)

    @measure_execution_time
    def run_query(self, df):
        count = df.count()
        logger.info(f"Query returned {count} rows.")


class QueryEfficiencyBenchmarkRunner(Benchmark):
    def __init__(self, format: str, benchmark_settings: BenchmarkSettings) -> None:
        self.benchmarks = {
            "delta": DeltaQueryEfficiencyBenchmark,
            "iceberg": IcebergQueryEfficiencyBenchmark,
            "hudi": HudiQueryEfficiencyBenchmark,
        }
        self.benchmark = self.benchmarks[format](benchmark_settings)  # type: ignore

    def start(self, spark: SparkSession) -> None:
        self.benchmark.start(spark)
