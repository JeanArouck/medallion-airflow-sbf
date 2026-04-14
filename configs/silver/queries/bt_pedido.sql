CREATE OR REPLACE TABLE `projeto-estudo-datalake.trusted.bt_pedido` (
  id	STRING,
  data_pedido_dt	DATE,
  month_id STRING,
  sgl_uf_entrega STRING,	
  vlr_total FLOAT64,
  aud_insert_dt DATE,
  aud_update_dt DATE
)
PARTITION BY data_pedido_dt
;

-- PROCESSO PARA ATUALIZAR/DEDUPLICAR A CAMADA SILVER, PREPARANDO PARA CAMADA ANALITICA

MERGE `projeto-estudo-datalake.trusted.bt_pedido` T
USING `projeto-estudo-datalake.raw.pedido` S
ON T.id = S.id AND S.aud_partition_dt = current_date()

WHEN MATCHED THEN
  UPDATE SET
    month_id = S.month_id,
    data_pedido_dt = S.data_pedido_dt,
    sgl_uf_entrega = S.sgl_uf_entrega,
    vlr_total = S.vlr_total,
    aud_insert_dt = T.aud_insert_dt,
    aud_update_dt = current_date()

WHEN NOT MATCHED THEN
  INSERT (
    id,
    month_id,
    data_pedido_dt,
    sgl_uf_entrega,
    vlr_total,
    aud_insert_dt,
    aud_update_dt
  )
  VALUES (
    S.id,
    S.month_id,
    S.data_pedido_dt,
    S.sgl_uf_entrega,
    S.vlr_total,
    current_date(),
    current_date()
  );

