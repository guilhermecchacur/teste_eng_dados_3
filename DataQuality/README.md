# DataQuality

Nessa etapa as validações de qualidade foram aplicadas sobre os dados da camada silver.
Foi aplicado as seguintes validações:
    . Quantidade de nulos em cada coluna
    . QUantidade de chaves duplicadas na PK
    . Quantidade de datas de atualização maiores que hoje

O resultado do Quality fica salvo como tabela, que pode ser imcorporado ao lake para consultas ou relatórios, como mostrado abaixo:
```text
+----------+--------------+----------+---------------------+--------------+-----+
|table     |partition_date|total_rows|column               |metric        |value|
+----------+--------------+----------+---------------------+--------------+-----+
|tb_cliente|2026-02-04    |397       |cod_cliente          |nulls         |0    |
|tb_cliente|2026-02-04    |397       |cod_cliente          |duplicated_key|0    |
|tb_cliente|2026-02-04    |397       |nm_cliente           |nulls         |0    |
|tb_cliente|2026-02-04    |397       |nm_pais_cliente      |nulls         |0    |
|tb_cliente|2026-02-04    |397       |nm_cidade_cliente    |nulls         |0    |
|tb_cliente|2026-02-04    |397       |nm_rua_cliente       |nulls         |0    |
|tb_cliente|2026-02-04    |397       |num_casa_cliente     |nulls         |0    |
|tb_cliente|2026-02-04    |397       |num_telefone_cliente |nulls         |35   |
|tb_cliente|2026-02-04    |397       |dt_nascimento_cliente|nulls         |0    |
|tb_cliente|2026-02-04    |397       |dt_atualizacao       |nulls         |0    |
|tb_cliente|2026-02-04    |397       |dt_atualizacao       |future_date   |0    |
|tb_cliente|2026-02-04    |397       |tp_pessoa            |nulls         |0    |
|tb_cliente|2026-02-04    |397       |vl_renda             |nulls         |0    |
+----------+--------------+----------+---------------------+--------------+-----+