import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional

import docker
import requests
from delta import configure_spark_with_delta_pip
from loguru import logger
from pyspark.sql import SparkSession


def create_delta_spark_session(app_name: str, spark_cluster_ip: str):
    builder = (
        SparkSession.builder.master(f"spark://{spark_cluster_ip}:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .appName(app_name)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
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


def download_dataset(dataset_url: str, destination_path: str) -> None:
    destination = Path(destination_path)

    if not destination.exists():
        response = requests.get(dataset_url)
        response.raise_for_status()

        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(BytesIO(response.content), "r") as zip:
            data = zip.open(destination.name, "r").read()
            with open(destination, "wb") as file:
                file.write(data)

        logger.info(f"Downloaded dataset to {destination}")
    else:
        logger.info(f"Dataset already exists at {destination}, download skipped.")


def get_project_root() -> Path:
    return Path(__file__).parent.parent
