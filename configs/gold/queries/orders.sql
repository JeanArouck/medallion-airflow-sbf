CREATE OR REPLACE TABLE `projeto-estudo-datalake.refined.orders` (
  id_pedido STRING,
  data_pedido_dt DATE,
  month_id STRING,
  sgl_uf_entrega STRING,
  vlr_total FLOAT64,
  vlr_unitario FLOAT64,
  qtd_produto INT64,
  id_produto STRING,
  nome_produto STRING,
  categoria_produto STRING,
  id_marca STRING,
  nome_marca STRING,
  aud_insert_dt DATE,
  aud_update_dt DATE
)
PARTITION BY data_pedido_dt
CLUSTER BY month_id, sgl_uf_entrega, categoria_produto, nome_marca
;


MERGE `projeto-estudo-datalake.refined.orders` T
USING (
    SELECT
      ped.id AS id_pedido,
      ped.data_pedido_dt,
      ped.month_id,
      ped.sgl_uf_entrega,
      ped.vlr_total,
      item.vlr_unitario,
      item.qtd_produto,
      prod.id AS id_produto,
      prod.nome AS nome_produto,
      prod.categoria AS categoria_produto,
      mrc.id AS id_marca,
      mrc.nome AS nome_marca
    FROM `projeto-estudo-datalake.trusted.bt_pedido` ped
    LEFT JOIN `projeto-estudo-datalake.trusted.bt_pedido_item` item
      ON ped.id = item.id_pedido
    LEFT JOIN `projeto-estudo-datalake.trusted.lk_produto` prod
      ON item.id_produto = prod.id
    LEFT JOIN `projeto-estudo-datalake.trusted.lk_marca` mrc
      ON prod.id_marca = mrc.id
) S
ON T.id_pedido = S.id_pedido
AND T.id_produto = S.id_produto
AND T.id_marca = S.id_marca

WHEN MATCHED THEN
  UPDATE SET
    id_pedido = S.id_pedido,
    data_pedido_dt = S.data_pedido_dt,
    month_id = S.month_id,
    sgl_uf_entrega = S.sgl_uf_entrega,
    vlr_total = S.vlr_total,
    vlr_unitario = S.vlr_unitario,
    qtd_produto = S.qtd_produto,
    id_produto = S.id_produto,
    nome_produto = S.nome_produto,
    categoria_produto = S.categoria_produto,
    id_marca = S.id_marca,
    nome_marca = S.nome_marca,

    aud_insert_dt = T.aud_insert_dt,
    aud_update_dt = current_date()

WHEN NOT MATCHED THEN
  INSERT (
    id_pedido,
    data_pedido_dt,
    month_id,
    sgl_uf_entrega,
    vlr_total,
    vlr_unitario,
    qtd_produto,
    id_produto,
    nome_produto,
    categoria_produto,
    id_marca,
    nome_marca,
    aud_insert_dt,
    aud_update_dt
  )
  VALUES (
    S.id_pedido,
    S.data_pedido_dt,
    S.month_id,
    S.sgl_uf_entrega,
    S.vlr_total,
    S.vlr_unitario,
    S.qtd_produto,
    S.id_produto,
    S.nome_produto,
    S.categoria_produto,
    S.id_marca,
    S.nome_marca,
    current_date(),
    current_date()
  );