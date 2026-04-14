

CREATE OR REPLACE TABLE `projeto-estudo-datalake.trusted.bt_meta` (
  month_id STRING,
  id_marca STRING,
  vlr_meta FLOAT64,
  aud_insert_dt DATE,
  aud_update_dt DATE
)
CLUSTER BY month_id
;

-- PROCESSO PARA ATUALIZAR/DEDUPLICAR A CAMADA SILVER, PREPARANDO PARA CAMADA ANALITICA
MERGE `projeto-estudo-datalake.trusted.bt_meta` T
USING `projeto-estudo-datalake.raw.meta` S
ON T.month_id = S.month_id
AND S.aud_partition_dt = current_date()

WHEN MATCHED THEN
  UPDATE SET
    month_id = S.month_id,
    id_marca = S.id_marca,
    vlr_meta = S.vlr_meta,
    aud_insert_dt = T.aud_insert_dt,
    aud_update_dt = current_date()

WHEN NOT MATCHED THEN
  INSERT (
    month_id,
    id_marca,
    vlr_meta,
    aud_insert_dt,
    aud_update_dt
  )
  VALUES (
    S.month_id,
    S.id_marca,
    S.vlr_meta,
    current_date(),
    current_date()
  );

