import click
from loguru import logger

from benchmarking import utils
from benchmarking.benchmarks import InvalidBenchmarkTypeException, get_benchmark_runner


@click.command()
@click.option(
    "--benchmark_type",
    type=click.Choice(
        ["time-travel", "ingestion", "initial-ingestion"], case_sensitive=False
    ),
    help="The benchmark to run.",
)
def main(benchmark_type):
    spark_cluster_ip = utils.get_container_ip("spark-master")
    spark = utils.create_delta_spark_session("BenchmarkRunner", spark_cluster_ip)

    try:
        benchmark_runner = get_benchmark_runner(benchmark_type)
        benchmark_runner.start(spark)
    except InvalidBenchmarkTypeException as e:
        logger.error(e)

    spark.stop()


if __name__ == "__main__":
    main()
