-- INGERE DADOS NA SILVER DE DADOS FISICOS PERSISTIDOS NA CAMADA RAW
CREATE OR REPLACE EXTERNAL TABLE `projeto-estudo-datalake.raw.tmp_marca`
OPTIONS (
  format = 'CSV',
  uris = ['gs://sbf-teste-raw-data/marca.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);


CREATE OR REPLACE TABLE `projeto-estudo-datalake.raw.marca` (
  id STRING,
  nome STRING,
  aud_partition_dt DATE,
  aud_source STRING
)
PARTITION BY aud_partition_dt
;

-- ESQUEMA PARA GRAVAR DADOS  COM CARGA DIARIA IDEMPOTENTE MANTENDO O HISTORICO DIARIZADO DAS CARGAS
-- Remove dados do dia atual
DELETE FROM `projeto-estudo-datalake.raw.marca`
WHERE aud_partition_dt = CURRENT_DATE();

-- Insere novamente
INSERT INTO `projeto-estudo-datalake.raw.marca` (
  id,
  nome,
  aud_partition_dt,
  aud_source
)
SELECT
  CAST(id AS STRING) AS id,
  nome,
  CURRENT_DATE() AS aud_partition_dt,
  'gs://sbf-teste-raw-data/marca.csv' AS aud_source
FROM `projeto-estudo-datalake.raw.tmp_marca`;


DROP TABLE `projeto-estudo-datalake.raw.tmp_marca`;

