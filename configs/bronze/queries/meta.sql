-- INGERE DADOS NA SILVER DE DADOS FISICOS PERSISTIDOS NA CAMADA RAW
CREATE OR REPLACE EXTERNAL TABLE `projeto-estudo-datalake.raw.tmp_meta`
OPTIONS (
  format = 'CSV',
  uris = ['gs://sbf-teste-raw-data/meta.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);


CREATE OR REPLACE TABLE `projeto-estudo-datalake.raw.meta` (
  month_id STRING,
  id_marca STRING,
  vlr_meta FLOAT64,
  aud_partition_dt DATE,
  aud_source STRING
)
PARTITION BY aud_partition_dt
;

-- ESQUEMA PARA GRAVAR DADOS  COM CARGA DIARIA IDEMPOTENTE MANTENDO O HISTORICO DIARIZADO DAS CARGAS
-- Remove dados do dia atual
DELETE FROM `projeto-estudo-datalake.raw.meta`
WHERE aud_partition_dt = CURRENT_DATE();

-- Insere novamente
INSERT INTO `projeto-estudo-datalake.raw.meta` (
  month_id,
  id_marca,
  vlr_meta,
  aud_partition_dt,
  aud_source
)
SELECT
  CONCAT(ano,"-",	mes) AS month_id,
  CAST(id_marca AS STRING) AS id_marca,
  vlr_meta,
  CURRENT_DATE() AS aud_partition_dt,
  'gs://sbf-teste-raw-data/meta.csv' AS aud_source
FROM `projeto-estudo-datalake.raw.tmp_meta`;


DROP TABLE `projeto-estudo-datalake.raw.tmp_meta`;

