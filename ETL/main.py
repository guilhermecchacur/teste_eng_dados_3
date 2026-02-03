from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window

from utils.utils import EtlConfig, Logger, BaseSpark, read_file, write_file


def glue_add_partition(spark, table_path, database, table, partition_column, partition_value):
    partition_path = f'{table_path}/{partition_column}={partition_value}'

    query = """
        alter table {}.{}
        add if not exists partition ({}='{}')
        location '{}'
        """.format(database, table, partition_column, partition_value, partition_path)
    
    spark.sql(query)


class EtlTransformations:
    @staticmethod
    def upper_all(df: DataFrame, column: str) -> DataFrame:
        """
        Uppercase all values in a given column.

        Args:
            df: Input Spark DataFrame.
            column: Column name to convert to uppercase.

        Returns:
            A new DataFrame with the given column uppercased.
        """
        return df.withColumn(column, F.upper(F.col(column)))

    @staticmethod
    def rename_column(df: DataFrame, column_old: str, column_new: str) -> DataFrame:
        """
        Rename a column.

        Args:
            df: Input Spark DataFrame.
            column_old: Existing column name.
            column_new: New column name.

        Returns:
            A new DataFrame with the column renamed.
        """
        return df.withColumnRenamed(column_old, column_new)

    @staticmethod
    def valid_phone(df: DataFrame, column: str) -> DataFrame:
        """
        Validate Brazilian phone format and set invalid values to null.

        Expected pattern: (NN)NNNNN-NNNN

        Args:
            df: Input Spark DataFrame.
            column: Column name that contains the phone number.

        Returns:
            A new DataFrame where invalid phone values are replaced with null.
        """
        regex = r"^\(\d{2}\)\d{5}-\d{4}$"
        return df.withColumn(
            column,
            F.when(F.col(column).rlike(regex), F.col(column)).otherwise(F.lit(None)),
        )

    @staticmethod
    def deduplication(
        df: DataFrame, groupby_column: str, orderby_column: str
    ) -> DataFrame:
        """
        Keep only the latest record per group, ordered by a date/timestamp column (descending).

        Args:
            df: Input Spark DataFrame.
            groupby_column: Column used to group records (e.g., customer id).
            orderby_column: Column used to decide the "latest" record (higher = more recent).

        Returns:
            A new DataFrame with only the most recent row per group.
        """
        window = Window.partitionBy(groupby_column).orderBy(F.col(orderby_column).desc())
        return (
            df.withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .drop("rank")
        )


class EtlPipeline:
    def __init__(self) -> None:
        """
        Initialize pipeline configuration, logger and Spark session.

        Raises:
            Exception: Re-raises any exception that happens during initialization after logging.
        """
        try:
            self.config = EtlConfig()
            self.name: str = self.config.name
            self.log = Logger(name=self.name)

            self.spark = BaseSpark(name=self.name).create_session()
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        except Exception as exc:
            try:
                self.log.exception("Failed to initialize ETL pipeline.")
            except Exception:
                pass
            raise

    def bronze_layer(self) -> None:
        """
        Bronze layer:
        - Read raw CSV
        - Basic standardization (uppercase and rename)
        - Add process_date
        - Write partitioned by process_date

        Raises:
            Exception: Re-raises any exception after logging.
        """
        date_today = datetime.now().strftime("%Y-%m-%d")
        self.log.info(f"Start Bronze Layer Process - {date_today}")

        try:
            df = read_file(spark=self.spark, file_type="csv", path=self.config.raw_file)

            df = EtlTransformations.upper_all(df, "nm_cliente")
            df = EtlTransformations.rename_column(
                df, "telefone_cliente", "num_telefone_cliente"
            )
            df = df.withColumn("anomesdia", F.lit(date_today))


            write_file(df, self.config.bucket_bronze, "anomesdia")

            glue_add_partition(
                spark=self.spark,
                table_path=self.config.bucket_bronze,
                database=self.config.database_bronze,
                table=self.config.tabela_bronze,
                partition_column='anomesdia',
                partition_value=date_today
            )

            self.log.info(f"Finished Bronze Layer Process - {date_today}")
            self.log.info(f"Saved in {self.config.bucket_bronze}\n")

        except Exception:
            self.log.exception(f"Bronze Layer failed - {date_today}")
            raise

    def silver_layer(self) -> None:
        """
        Silver layer:
        - Read bronze parquet
        - Validate phone column
        - Cast/convert fields to proper types
        - Deduplicate by latest dt_atualizacao per cod_cliente
        - Add process_date
        - Write partitioned by process_date

        Raises:
            Exception: Re-raises any exception after logging.
        """
        date_today = datetime.now().strftime("%Y-%m-%d")
        self.log.info(f"Start Silver Layer Process - {date_today}")

        try:
            df = read_file(
                spark=self.spark, file_type="parquet", path=self.config.bucket_bronze
            )

            df = EtlTransformations.valid_phone(df, "num_telefone_cliente")

            df = (
                df.withColumn(
                    "dt_atualizacao",
                    F.to_date(F.col("dt_atualizacao"), "yyyy-MM-dd"),
                )
                .withColumn(
                    "dt_nascimento_cliente",
                    F.to_date(F.col("dt_nascimento_cliente"), "yyyy-MM-dd"),
                )
                .withColumn("vl_renda", F.col("vl_renda").cast(DecimalType(10, 2)))
                .withColumn(
                    "num_casa_cliente", F.col("num_casa_cliente").cast(IntegerType())
                )
            )

            df = EtlTransformations.deduplication(df, "cod_cliente", "dt_atualizacao")
            df = df.withColumn("anomesdia", F.lit(date_today))

            write_file(df, self.config.bucket_silver, "anomesdia")

            glue_add_partition(
                spark=self.spark,
                table_path=self.config.bucket_silver,
                database=self.config.database_silver,
                table=self.config.tabela_silver,
                partition_column='anomesdia',
                partition_value=date_today
            )

            self.log.info(f"Finished Silver Layer Process - {date_today}")
            self.log.info(f"Saved in {self.config.bucket_silver}\n")

        except Exception:
            self.log.exception(f"Silver Layer failed - {date_today}")
            raise


    def run(self) -> None:
        """
        Run the full ETL pipeline (Bronze -> Silver) and stop Spark session.

        Raises:
            Exception: Re-raises any exception after logging; Spark stop attempted in finally.
        """
        try:
            self.bronze_layer()
            self.silver_layer()
        except Exception:
            self.log.exception("ETL run failed.")
            raise
        finally:
            try:
                self.spark.stop()
                self.log.info("Spark session stopped.")
            except Exception:
                self.log.exception("Failed to stop Spark session cleanly.")


if __name__ == "__main__":
    EtlPipeline().run()