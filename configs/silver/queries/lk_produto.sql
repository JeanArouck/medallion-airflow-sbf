CREATE OR REPLACE TABLE `projeto-estudo-datalake.trusted.lk_produto` (
  id STRING,
  id_marca STRING,
  nome STRING,
  categoria STRING,
  aud_insert_dt DATE,
  aud_update_dt DATE
)
PARTITION BY aud_insert_dt
CLUSTER BY categoria;


MERGE `projeto-estudo-datalake.trusted.lk_produto` T
USING `projeto-estudo-datalake.raw.produto` S
ON T.id = S.id

WHEN MATCHED THEN
  UPDATE SET
    id = S.id,
    id_marca = S.id_marca,
    nome = S.nome,
    categoria = S.categoria,
    aud_insert_dt = T.aud_insert_dt,
    aud_update_dt = current_date()

WHEN NOT MATCHED THEN
  INSERT (
    id,
    id_marca,
    nome,
    categoria,
    aud_insert_dt,
    aud_update_dt
  )
  VALUES (
    S.id,
    S.id_marca,
    S.nome,
    S.categoria,
    current_date(),
    current_date()
  );