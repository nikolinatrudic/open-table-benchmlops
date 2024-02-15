from typing import Optional

import docker
from loguru import logger


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
