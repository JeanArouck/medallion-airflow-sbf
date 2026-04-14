
-- INGERE DADOS NA SILVER DE DADOS FISICOS PERSISTIDOS NA CAMADA RAW
CREATE OR REPLACE EXTERNAL TABLE `projeto-estudo-datalake.raw.tmp_pedido`
OPTIONS (
  format = 'CSV',
  uris = ['gs://sbf-teste-raw-data/pedido.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);


CREATE OR REPLACE TABLE `projeto-estudo-datalake.raw.pedido` (
  id	STRING,
  data_pedido_dt	DATE,
  month_id STRING,
  sgl_uf_entrega STRING,	
  vlr_total FLOAT64,
  aud_partition_dt DATE,
  aud_source STRING
)
PARTITION BY aud_partition_dt
;

-- ESQUEMA PARA GRAVAR DADOS  COM CARGA DIARIA IDEMPOTENTE MANTENDO O HISTORICO DIARIZADO DAS CARGAS
-- Remove dados do dia atual
DELETE FROM `projeto-estudo-datalake.raw.pedido`
WHERE aud_partition_dt = CURRENT_DATE();

-- Insere novamente
INSERT INTO `projeto-estudo-datalake.raw.pedido` (
  id,
  month_id,
  data_pedido_dt,
  sgl_uf_entrega,
  vlr_total,
  aud_partition_dt,
  aud_source
)
SELECT

  CAST(id AS STRING) AS id,
  FORMAT_DATE('%Y-%-m', data) AS month_id,
  data AS data_pedido_dt,
  sgl_uf_entrega,
  vlr_total,
  CURRENT_DATE() AS aud_partition_dt,
  'gs://sbf-teste-raw-data/pedido.csv' AS aud_source
FROM `projeto-estudo-datalake.raw.tmp_pedido`;


DROP TABLE `projeto-estudo-datalake.raw.tmp_pedido`;

