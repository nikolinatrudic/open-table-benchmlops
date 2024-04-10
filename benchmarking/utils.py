import time
from pathlib import Path
from typing import Optional

import docker
from delta import configure_spark_with_delta_pip
from loguru import logger
from pyspark.sql import SparkSession


def create_delta_spark_session(app_name: str, spark_master: str) -> SparkSession:
    builder = (
        SparkSession.builder.master(spark_master)
        .config("spark.eventLog.enabled", True)
        .config("spark.eventLog.dir", "/tmp/spark-events")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config(
            "spark.hadoop.fs.s3a.endpoint", "http://localhost.localstack.cloud:4566"
        )
        .appName(app_name)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def create_iceberg_spark_session(app_name: str, spark_master: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.eventLog.enabled", True)
        .config("spark.eventLog.dir", "/tmp/spark-events")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            "s3a://data",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config(
            "spark.hadoop.fs.s3a.endpoint", "http://localhost.localstack.cloud:4566"
        )
        .getOrCreate()
    )
    return spark


def create_hudi_spark_session(app_name: str, spark_master: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.eventLog.enabled", True)
        .config("spark.eventLog.dir", "/tmp/hudi-spark-events")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hudi:hudi-spark3-bundle_2.12:0.14.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config(
            "spark.hadoop.fs.s3a.endpoint", "http://localhost.localstack.cloud:4566"
        )
        .getOrCreate()
    )

    return spark


def get_container_ip(container_name: str) -> Optional[str]:
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        networks = container.attrs["NetworkSettings"]["Networks"]

        ip_address = None
        for network_name, network_details in networks.items():
            ip_address = network_details["IPAddress"]

        return ip_address
    except docker.errors.NotFound:
        logger.error(f"Container {container_name} not found.")
        return None
    except Exception as e:
        logger.error(f"Error {e} happened.")
        return None


def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(
            f"Execution time for {func.__name__}: {end_time - start_time:.2f} seconds"
        )
        return result

    return wrapper


def get_project_root() -> Path:
    return Path(__file__).parent.parent
