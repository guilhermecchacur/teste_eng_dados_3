import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


class BaseSpark:
    """
    Small wrapper to create a SparkSession.
    """

    def __init__(self, name: str) -> None:
        """
        Args:
            name: Application name for the SparkSession.
        """
        self.name = name

    def create_session(self) -> SparkSession:
        """
        Create and return a local SparkSession.

        Returns:
            A SparkSession instance.

        Raises:
            RuntimeError: If SparkSession creation fails.
        """
        try:
            return (
                SparkSession.builder.appName(self.name)
                .master("local[*]")
                .getOrCreate()
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to create SparkSession for app '{self.name}'.") from exc


class Logger:
    """
    Minimal logger wrapper to standardize format and avoid duplicated handlers in notebooks.
    """

    def __init__(self, name: str, level: int = logging.INFO) -> None:
        """
        Args:
            name: Logger name (usually the job/pipeline name).
            level: Logging level (default: INFO).
        """
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._logger.propagate = False

        # Important in notebooks: avoid accumulating handlers & formatters across re-runs
        self._logger.handlers.clear()

        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def info(self, message: str) -> None:
        """
        Log an INFO message.

        Args:
            message: Message to log.

        Returns:
            None
        """
        self._logger.info(message)

    def error(self, message: str) -> None:
        """
        Log an ERROR message.

        Args:
            message: Message to log.

        Returns:
            None
        """
        self._logger.error(message)

    def exception(self, message: str) -> None:
        """
        Log an ERROR message including stack trace (use inside except blocks).

        Args:
            message: Message to log.

        Returns:
            None
        """
        self._logger.exception(message)


class EtlConfig:
    """
    Loads config.json from the same folder as this file and exposes keys as attributes.
    """

    def __init__(self, config_path: str) -> None:
        """
        Args:
            config_path: Config file path.

        Raises:
            FileNotFoundError: If config file does not exist.
            ValueError: If config tries to overwrite reserved attributes.
            RuntimeError: If JSON parsing fails.
        """
        base_path = Path(__file__).resolve()
        project_root = base_path.parent

        self.path: Path = Path(config_path)
        data = self._load()

        # Turn JSON keys into attributes
        self._set_attributes(**data)

    def _load(self) -> dict:
        """
        Read and parse the JSON config file.

        Returns:
            Dict containing config values.

        Raises:
            FileNotFoundError: If config file is missing.
            RuntimeError: If JSON is invalid or cannot be read.
        """
        if not self.path.exists():
            raise FileNotFoundError(f"Config not found: {self.path}")

        try:
            with self.path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON in config file: {self.path}") from exc
        except OSError as exc:
            raise RuntimeError(f"Failed to read config file: {self.path}") from exc

    def _set_attributes(self, **kwargs) -> None:
        """
        Set config keys as attributes on this instance.

        Args:
            **kwargs: Key/value pairs from the config JSON.

        Returns:
            None

        Raises:
            ValueError: If config key would overwrite an existing attribute.
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                raise ValueError(
                    f"Config key '{key}' conflicts with an existing attribute in EtlConfig."
                )
            setattr(self, key, value)


def read_file(
    spark: SparkSession,
    file_type: str,
    path: str,
    schema = None,
) -> DataFrame:
    """
    Read a file into a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        file_type: File type ('csv' or 'parquet').
        path: Input path.
        schema: Optional Spark schema (StructType) for CSV reading.

    Returns:
        DataFrame read from the given path.

    Raises:
        ValueError: If file_type is not supported.
        RuntimeError: If Spark fails to read the file.
    """
    try:
        if file_type == "csv":
            reader = spark.read.option("header", "true")
            if schema is not None:
                return reader.schema(schema).csv(path)
            return reader.option("inferSchema", "true").csv(path)

        if file_type == "parquet":
            return spark.read.parquet(path)

        raise ValueError(f"Unsupported file_type: {file_type}")

    except Exception as exc:
        # Add context but don't swallow; let the orchestrator decide what to do.
        raise RuntimeError(f"Failed to read {file_type} from path: {path}") from exc


def write_file(df: DataFrame, path: str, partition_column: str) -> None:
    """
    Write a DataFrame as parquet partitioned by a given column (overwrite mode).

    Args:
        df: DataFrame to write.
        path: Output path.
        partition_column: Column name used for partitioning.

    Returns:
        None

    Raises:
        ValueError: If partition column does not exist in the DataFrame.
        RuntimeError: If Spark fails to write the data.
    """
    if partition_column not in df.columns:
        raise ValueError(f"Partition column '{partition_column}' not found in DataFrame columns.")

    try:
        (
            df.write.mode("overwrite")
            .partitionBy(partition_column)
            .parquet(path)
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write parquet to path: {path} partitioned by '{partition_column}'."
        ) from exc
    
def parse_args():
    """
    Parse command-line arguments for the ETL job.

    Returns:
        argparse.Namespace: Parsed arguments containing:
            - config (str | None): Path to the configuration JSON file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        help="Path to config.json (local filesystem)",
        required=False,
    )
    return parser.parse_args()