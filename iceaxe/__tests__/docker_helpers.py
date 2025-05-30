"""
Docker helper utilities for testing.

This module provides classes and functions to manage Docker containers for testing,
particularly focusing on PostgreSQL database containers.
"""

import logging
import socket
import time
import uuid
from typing import Any, Dict, Optional, cast

import docker
from docker.errors import APIError

# Configure logging
logger = logging.getLogger(__name__)


def get_free_port() -> int:
    """Find a free port on the host machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class PostgresContainer:
    """
    A class that manages a PostgreSQL Docker container for testing.

    This class handles the lifecycle of a PostgreSQL container, including:
    - Starting the container with appropriate configuration
    - Finding available ports
    - Waiting for the container to be ready
    - Providing connection information
    - Cleaning up after tests
    """

    def __init__(
        self,
        pg_user: str = "iceaxe",
        pg_password: str = "mysecretpassword",
        pg_db: str = "iceaxe_test_db",
        postgres_version: str = "16",
    ):
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_db = pg_db
        self.postgres_version = postgres_version
        self.port = get_free_port()
        self.container: Optional[Any] = None
        self.client = docker.from_env()
        self.container_name = f"iceaxe-postgres-test-{uuid.uuid4().hex[:8]}"

    def start(self) -> Dict[str, Any]:
        """
        Start the PostgreSQL container.

        Returns:
            Dict[str, Any]: Connection information for the PostgreSQL container

        Raises:
            RuntimeError: If the container fails to start or become ready
        """
        logger.info(f"Starting PostgreSQL container on port {self.port}")

        max_attempts = 3
        attempt = 0

        while attempt < max_attempts:
            attempt += 1
            try:
                self.container = self._run_container(self.port)
                break
            except APIError as e:
                if "port is already allocated" in str(e) and attempt < max_attempts:
                    logger.warning(
                        f"Port {self.port} is still in use. Trying with a new port (attempt {attempt}/{max_attempts})."
                    )
                    self.port = get_free_port()
                else:
                    raise RuntimeError(f"Failed to start PostgreSQL container: {e}")

        # Wait for PostgreSQL to be ready
        if not self._wait_for_container_ready():
            self.stop()
            raise RuntimeError("Failed to connect to PostgreSQL container")

        return self.get_connection_info()

    def _run_container(
        self, port: int
    ) -> Any:  # Type as Any since docker.models.containers.Container isn't imported
        """
        Run the Docker container with the specified port.

        Args:
            port: The port to map PostgreSQL to on the host

        Returns:
            The Docker container object
        """
        return self.client.containers.run(
            f"postgres:{self.postgres_version}",
            name=self.container_name,
            detach=True,
            environment={
                "POSTGRES_USER": self.pg_user,
                "POSTGRES_PASSWORD": self.pg_password,
                "POSTGRES_DB": self.pg_db,
                # Additional settings for faster startup in testing
                "POSTGRES_HOST_AUTH_METHOD": "trust",
            },
            ports={"5432/tcp": port},
            remove=True,  # Auto-remove container when stopped
        )

    def _wait_for_container_ready(self) -> bool:
        """
        Wait for the PostgreSQL container to be ready.

        Returns:
            bool: True if the container is ready, False otherwise
        """
        max_retries = 30
        retry_interval = 1

        for i in range(max_retries):
            try:
                if self.container is None:
                    logger.warning("Container is None, cannot proceed")
                    return False

                # We've already checked that self.container is not None
                container = cast(Any, self.container)
                container.reload()  # Refresh container status
                if container.status != "running":
                    logger.warning(f"Container status: {container.status}")
                    return False

                # Try to connect to PostgreSQL
                conn = socket.create_connection(("localhost", self.port), timeout=1)
                conn.close()
                # Wait a bit more to ensure PostgreSQL is fully initialized
                time.sleep(2)
                logger.info(f"PostgreSQL container is ready after {i + 1} attempt(s)")
                return True
            except (socket.error, ConnectionRefusedError) as e:
                if i == max_retries - 1:
                    logger.warning(
                        f"Failed to connect after {max_retries} attempts: {e}"
                    )
                    return False
                time.sleep(retry_interval)
            except Exception as e:
                logger.warning(f"Unexpected error checking container readiness: {e}")
                if i == max_retries - 1:
                    return False
                time.sleep(retry_interval)

        return False

    def stop(self) -> None:
        """
        Stop the PostgreSQL container.

        This method ensures the container is properly stopped and removed.
        """
        if self.container is not None:
            try:
                logger.info(f"Stopping PostgreSQL container {self.container_name}")
                # We've already checked that self.container is not None
                container = cast(Any, self.container)
                container.stop(timeout=10)  # Allow 10 seconds for graceful shutdown
            except Exception as e:
                logger.warning(f"Failed to stop container: {e}")
                try:
                    # Force remove as a fallback
                    if self.container is not None:
                        self.container.remove(force=True)
                    logger.info("Forced container removal")
                except Exception as e2:
                    logger.warning(f"Failed to force remove container: {e2}")

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get the connection information for the PostgreSQL container.

        Returns:
            Dict[str, Any]: A dictionary containing connection parameters
        """
        return {
            "host": "localhost",
            "port": self.port,
            "user": self.pg_user,
            "password": self.pg_password,
            "database": self.pg_db,
        }

    def get_connection_string(self) -> str:
        """
        Get a PostgreSQL connection string.

        Returns:
            str: A connection string in the format 'postgresql://user:password@host:port/database'
        """
        return f"postgresql://{self.pg_user}:{self.pg_password}@localhost:{self.port}/{self.pg_db}"
