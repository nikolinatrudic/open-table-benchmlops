import click
from loguru import logger
from pyspark.sql import SparkSession

from benchmark.utils import get_container_ip


def create_spark_session(app_name: str, spark_cluster_ip: str):
    spark = (
        SparkSession.builder.master(f"spark://{spark_cluster_ip}:7077")
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def run_time_travel_benchmark(spark):
    logger.info("Running time travel benchmark")


def run_ingestion_benchmark(spark):
    logger.info("Running ingestion benchmark")


@click.command()
@click.option(
    "--benchmark",
    type=click.Choice(["time-travel", "ingestion"], case_sensitive=False),
    help="The benchmark to run.",
)
def main(benchmark):
    spark_cluster_ip = get_container_ip("spark-master")
    spark = create_spark_session("BenchmarkRunner", spark_cluster_ip)

    match benchmark:
        case "time-travel":
            run_time_travel_benchmark(spark)
        case "ingestion":
            run_ingestion_benchmark(spark)
        case _:
            logger.warning("Please specify a valid benchmark to run.")

    spark.stop()


if __name__ == "__main__":
    main()
