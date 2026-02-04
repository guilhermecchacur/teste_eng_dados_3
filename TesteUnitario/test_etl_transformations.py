import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from ETL.main import EtlTransformations


@pytest.fixture(scope='session')
def spark():
    spark = (
        SparkSession.builder
        .master('local[2]')
        .appName('pytest-etl-transformations')
        .getOrCreate()
    )
    yield spark
    spark.stop()



def test_upper_all_happy_path(spark):
    df = spark.createDataFrame(
        [('joao',), ('Maria',), ('PeDro',)],
        ['nm_cliente']
    )

    out = EtlTransformations.upper_all(df, 'nm_cliente')
    result = [r['nm_cliente'] for r in out.orderBy('nm_cliente').collect()]

    assert result == ['JOAO', 'MARIA', 'PEDRO']



def test_valid_phone_edge_case_null_and_empty(spark):
    df = spark.createDataFrame(
        [
            ('(11)91234-5678',),  
            (None,),              
            ('',),                
            ('(11)912345-678',),  
        ],
        ['num_telefone_cliente']
    )

    out = EtlTransformations.valid_phone(df, 'num_telefone_cliente')

    got = [r['num_telefone_cliente'] for r in out.collect()]
    assert got[0] == '(11)91234-5678'
    assert got[1] is None
    assert got[2] is None
    assert got[3] is None



def test_rename_column_error_when_column_missing(spark):
    df = spark.createDataFrame([(1,)], ['col_existente'])

   
    
    out = EtlTransformations.rename_column(df, 'col_inexistente', 'novo_nome')

    with pytest.raises(Exception):
        out.select('novo_nome').collect()



def test_deduplication_happy_path(spark):
    df = spark.createDataFrame(
        [
            (1, '2025-01-01', 'A'),
            (1, '2025-01-10', 'B'),  
            (2, '2025-02-01', 'C'),
            (2, '2025-01-01', 'D'),
        ],
        ['cod_cliente', 'dt_atualizacao', 'valor']
    ).withColumn('dt_atualizacao', F.to_date('dt_atualizacao', 'yyyy-MM-dd'))

    out = EtlTransformations.deduplication(df, 'cod_cliente', 'dt_atualizacao')

    rows = {(r['cod_cliente'], str(r['dt_atualizacao']), r['valor']) for r in out.collect()}
    assert rows == {
        (1, '2025-01-10', 'B'),
        (2, '2025-02-01', 'C'),
    }
