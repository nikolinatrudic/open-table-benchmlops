import click
from loguru import logger

from benchmarking import utils
from benchmarking.benchmarks import InvalidBenchmarkTypeException, get_benchmark_runner
from benchmarking.settings import benchmark_settings

spark_sessions = {
    "delta": utils.create_delta_spark_session,
    "iceberg": utils.create_iceberg_spark_session,
    "hudi": utils.create_hudi_spark_session,
}


@click.command()
@click.option(
    "--benchmark_type",
    type=click.Choice(
        ["initial-ingestion", "ingestion", "time-travel", "query-efficiency"],
        case_sensitive=False,
    ),
    help="The benchmark to run.",
)
@click.option(
    "--format",
    type=click.Choice(
        ["delta", "iceberg", "hudi"],
        case_sensitive=False,
    ),
    help="The format for which to run benchmark.",
)
@click.option(
    "--spark_master",
    type=click.STRING,
    help="The spark master address.",
)
def main(benchmark_type: str, format: str, spark_master: str):
    spark = spark_sessions[format](
        f"{format}-benchmark-runner-{benchmark_type}", spark_master
    )

    try:
        benchmark_runner = get_benchmark_runner(
            benchmark_type, format, benchmark_settings
        )
        benchmark_runner.start(spark)
        spark.stop()
    except InvalidBenchmarkTypeException as e:
        logger.error(e)
        spark.stop()


if __name__ == "__main__":
    main()
