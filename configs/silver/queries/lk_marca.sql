CREATE OR REPLACE TABLE `projeto-estudo-datalake.trusted.lk_marca` (
  id STRING,
  nome STRING,
  aud_insert_dt DATE,
  aud_update_dt DATE
)
PARTITION BY aud_insert_dt
;


MERGE `projeto-estudo-datalake.trusted.lk_marca` T
USING `projeto-estudo-datalake.raw.marca` S
ON T.id = S.id
AND S.aud_partition_dt = current_date()

WHEN MATCHED THEN
  UPDATE SET
    id = S.id,
    nome = S.nome,
    aud_insert_dt = T.aud_insert_dt,
    aud_update_dt = current_date()

WHEN NOT MATCHED THEN
  INSERT (
    id,
    nome,
    aud_insert_dt,
    aud_update_dt
  )
  VALUES (
    S.id,
    S.nome,
    current_date(),
    current_date()
  );