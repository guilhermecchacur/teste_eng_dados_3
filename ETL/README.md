# ETL

O arquivo bruto clientes_sinteticos.csv serviu de ingestão para as duas camadas de exemplo do exercício (bronze e silver).

Na camada bronze foi feito algumas transformações e ingerido na tabela tabela_cliente_landing, usando a partição física de data de processamento ao salvar. O schema foi mantido sem alterações herdado do csv.

Na camada silver foram feito outras transformações ingerido na tabela tb_cliente, usando a partição física de data de processamento ao salvar. Houve alteração no schema para adequar algumas colunas ao datatype julgado correto.
