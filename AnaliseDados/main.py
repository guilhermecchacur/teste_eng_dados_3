from utils.utils import BaseSpark, read_file

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

# Input file path
file_path = '../clientes_sinteticos.csv'


def most_updated(df: DataFrame) -> DataFrame:
    """
    Returns the top 5 customers with the highest number of records.

    Args:
        df: DataFrame containing customer data.

    Returns:
        DataFrame with the most frequent customers.
    """
    return (
        df.groupBy('cod_cliente')
          .agg(F.count('cod_cliente').alias('qnt'))
          .orderBy(F.col('qnt').desc())
          .limit(5)
    )


def deduplication(
    df: DataFrame, groupby_column: str, orderby_column: str
) -> DataFrame:
    """
    Removes duplicate records keeping only the most recent one per group.

    Args:
        df: Input DataFrame.
        groupby_column: Column used to group records (e.g. customer id).
        orderby_column: Column used to define the most recent record.

    Returns:
        Deduplicated DataFrame.
    """
    window = Window.partitionBy(groupby_column).orderBy(F.col(orderby_column).desc())

    return (
        df.withColumn("rank", F.row_number().over(window))
          .filter(F.col("rank") == 1)
          .drop("rank")
    )


def avg_age_from_birth_date(df: DataFrame, birth_date_column: str) -> DataFrame:
    """
    Calculates the average age of customers based on birth date.

    Args:
        df: DataFrame containing birth date information.
        birth_date_column: Name of the birth date column.

    Returns:
        DataFrame with a single column containing the average age.
    """
    if not isinstance(df.schema[birth_date_column].dataType, DateType):
        df = df.withColumn(
            birth_date_column,
            F.to_date(birth_date_column, 'yyyy-MM-dd')
        )

    df = df.withColumn(
        'age',
        F.year(F.current_date()) - F.year(F.col(birth_date_column))
    )
    return df.agg(F.round(F.avg('age'), 0).alias('avg_age'))


def main():
    """
    Main execution function.
    """
    spark = BaseSpark(name='analise_dados').create_session()

    df = read_file(spark=spark, file_type='csv', path=file_path)

    df_most_updated = most_updated(df=df)
    df_most_updated.show()

    df_avg_age = deduplication(
        df=df,
        groupby_column='cod_cliente',
        orderby_column='dt_atualizacao'
    )

    df_avg_age = avg_age_from_birth_date(
        df=df_avg_age,
        birth_date_column='dt_nascimento_cliente'
    )

    df_avg_age.show()


if __name__ == '__main__':
    main()