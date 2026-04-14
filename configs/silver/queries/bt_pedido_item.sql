CREATE OR REPLACE TABLE `projeto-estudo-datalake.trusted.bt_pedido_item` (
  id_pedido	STRING,
  id_produto STRING,
  vlr_unitario FLOAT64,
  qtd_produto	INT64,
  flg_cancelado STRING,
  aud_insert_dt DATE,
  aud_update_dt DATE
)
PARTITION BY aud_insert_dt
;


-- PROCESSO PARA ATUALIZAR/DEDUPLICAR A CAMADA SILVER, PREPARANDO PARA CAMADA ANALITICA
MERGE `projeto-estudo-datalake.trusted.bt_pedido_item` T
USING `projeto-estudo-datalake.raw.pedido_item` S
ON T.id_pedido = S.id_pedido
AND S.aud_partition_dt = current_date()


WHEN MATCHED THEN
  UPDATE SET
    id_pedido = S.id_pedido,
    id_produto = S.id_produto,
    vlr_unitario = S.vlr_unitario,
    qtd_produto = S.qtd_produto,
    flg_cancelado = S.flg_cancelado,
    aud_insert_dt = T.aud_insert_dt,
    aud_update_dt = current_date()

WHEN NOT MATCHED THEN
  INSERT (
    id_pedido,
    id_produto,
    vlr_unitario,
    qtd_produto,
    flg_cancelado,
    aud_insert_dt,
    aud_update_dt
  )
  VALUES (
    S.id_pedido,
    S.id_produto,
    S.vlr_unitario,
    S.qtd_produto,
    S.flg_cancelado,
    current_date(),
    current_date()
  );