-- INGERE DADOS NA SILVER DE DADOS FISICOS PERSISTIDOS NA CAMADA RAW
CREATE OR REPLACE EXTERNAL TABLE `projeto-estudo-datalake.raw.tmp_pedido_item`
OPTIONS (
  format = 'CSV',
  uris = ['gs://sbf-teste-raw-data/pedido_item.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);


CREATE OR REPLACE TABLE `projeto-estudo-datalake.raw.pedido_item` (
  id_pedido	STRING,
  id_produto STRING,
  vlr_unitario FLOAT64,
  qtd_produto	INT64,
  flg_cancelado STRING,

  aud_partition_dt DATE,
  aud_source STRING
)
PARTITION BY aud_partition_dt
;

-- ESQUEMA PARA GRAVAR DADOS  COM CARGA DIARIA IDEMPOTENTE MANTENDO O HISTORICO DIARIZADO DAS CARGAS
-- Remove dados do dia atual
DELETE FROM `projeto-estudo-datalake.raw.pedido_item`
WHERE aud_partition_dt = CURRENT_DATE();

-- Insere novamente
INSERT INTO `projeto-estudo-datalake.raw.pedido_item` (
  id_pedido,
  id_produto,
  vlr_unitario,
  qtd_produto,
  flg_cancelado,
  aud_partition_dt,
  aud_source
)
SELECT

  CAST(id_pedido AS STRING) AS id_pedido,
  CAST(id_produto AS STRING) AS id_produto,
  vlr_unitario,
  qtd_produto,
  flg_cancelado,
  CURRENT_DATE() AS aud_partition_dt,
  'gs://sbf-teste-raw-data/pedido_item.csv' AS aud_source
FROM `projeto-estudo-datalake.raw.tmp_pedido_item`;


DROP TABLE `projeto-estudo-datalake.raw.tmp_pedido_item`;

