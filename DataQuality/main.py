from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Row
from pyspark.sql.window import Window

from utils.utils import EtlConfig, Logger, BaseSpark, read_file, write_file, parse_args


class Qualities:
    """
    Collection of static data quality checks.
    """

    @staticmethod
    def qnt_nulls(df: DataFrame, column: str) -> int:
        """
        Count null values in a column.

        Args:
            df: Input Spark DataFrame.
            column: Column name to check.

        Returns:
            Number of null values found in the column.
        """
        return df.filter(F.col(column).isNull()).count()
    
    @staticmethod
    def duplicated_key(df: DataFrame, column: str) -> int:
        """
        Count duplicated values in a key column.

        Args:
            df: Input Spark DataFrame.
            column: Key column name.

        Returns:
            Number of duplicated keys.
        """
        df = df.groupby(column).agg(F.count(column).alias('qnt'))
        return df.filter(F.col('qnt') > 1).count()
    
    @staticmethod
    def future_updated(df: DataFrame, column: str) -> int:
        """
        Count records with invalid update dates (future dates).

        Args:
            df: Input Spark DataFrame.
            column: Update date column to be validated.

        Returns:
            Number of records with update date greater than the current date.
        """
        return df.filter(F.col(column) > F.current_date()).count()
        

class QualityPipeline:
    """
    Pipeline responsible for executing data quality checks.
    """

    def __init__(self, config):
        """
        Initialize pipeline configuration, logger and Spark session.

        Args:
            config: config files for process parameters

        Returns:
            None
        """
        try:
            self.config = config
            self.name: str = self.config.name
            self.log = Logger(name=self.name)

            self.spark = BaseSpark(name=self.name).create_session()
            self.spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
            self.spark.conf.set('spark.sql.shuffle.partitions', '16')
            self.spark.conf.set('spark.default.parallelism', '8')

        except Exception:
            try:
                self.log.exception('Failed to initialize ETL pipeline.')
            except Exception:
                pass
            raise
    
    def run(self) -> None:
        """
        Execute data quality checks for the current date partition.

        Args:
            None

        Returns:
            None
        """
        date_today = datetime.now().strftime("%Y-%m-%d")
        partition_path = f'{self.config.bucket}/{self.config.partition_column}={date_today}'

        self.log.info(
            f'Starting Quality Check for {self.config.tabela} in {date_today} partition'
        )

        df = read_file(
            spark=self.spark,
            file_type='parquet',
            path=partition_path
        )

        df.cache()
        num_rows = df.count()

        rows = []
        for c in df.columns:
            self.log.info(f'Checking column {c}')
            nulls = Qualities.qnt_nulls(df=df, column=c)

            rows.append(Row(
                table=self.config.tabela,
                partition_date=date_today,
                total_rows=num_rows,
                column=c,
                metric='nulls',
                value=int(nulls) if nulls is not None else None
            ))

            if c == self.config.key_column:
                dup = Qualities.duplicated_key(
                    df=df,
                    column=self.config.key_column
                )

                rows.append(Row(
                    table=self.config.tabela,
                    partition_date=date_today,
                    total_rows=num_rows,
                    column=c,
                    metric='duplicated_key',
                    value=int(dup) if dup is not None else None
                ))

            if c == self.config.update_column:
                fut = Qualities.future_updated(
                    df=df,
                    column=self.config.update_column
                )

                rows.append(Row(
                    table=self.config.tabela,
                    partition_date=date_today,
                    total_rows=num_rows,
                    column=c,
                    metric='future_date',
                    value=int(fut) if fut is not None else None
                ))

        quality_report_df = self.spark.createDataFrame(rows)

        quality_report_df.show(truncate=False)

        write_file(
            df=quality_report_df,
            path=self.config.bucket_quality,
            partition_column='partition_date'
        )

        self.log.info(f'Finished Quality Check for {self.config.tabela} in {date_today} partition')

def main():
    args = parse_args()

    try:
        config = EtlConfig(config_path=args.config)
    except Exception as e:
        raise e
    
    QualityPipeline(config=config).run()

if __name__ == '__main__':
    main()