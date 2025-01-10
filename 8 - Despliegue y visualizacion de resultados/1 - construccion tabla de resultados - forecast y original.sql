
CREATE TABLE `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo_tablero` AS 
with taric_base as (
  SELECT
    fecha,
    tipo_movimiento_cod,
    cod_pais,
    'dolares' AS unidades,
    SUM(dolares) AS valor
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo`
  WHERE nivel_taric = '1'
  GROUP BY 1, 2, 3, 4
  UNION ALL
  SELECT
    fecha,
    tipo_movimiento_cod,
    cod_pais,
    'kilogramos' AS unidades,
    SUM(kilogramos) AS valor
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo`
  WHERE nivel_taric = '1'
  GROUP BY 1, 2, 3, 4
),
taric_global AS (
  SELECT
    fecha,
    tipo_movimiento_cod,
    unidades,
    SUM(valor) AS valor
  FROM taric_base
  GROUP BY 1, 2, 3
),
taric_general_forecast AS (
  SELECT
    fecha,
    CASE
      WHEN llave3 = 'GLOBAL' THEN 'global'
      ELSE 'pais'
    END AS nivel,
    llave2 AS tipo_movimiento_cod,
    CASE
      WHEN llave3 = 'GLOBAL' THEN '0000'
      ELSE llave3
    END AS cod_pais,
    llave1 AS unidades,
    prediccion AS valor,
    modelo
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_forecast`
  WHERE llave2 <> 'producto'
)
SELECT
  fecha,
  'pais' AS nivel,
  tipo_movimiento_cod,
  cod_pais,
  unidades,
  valor,
  'original' AS modelo
FROM taric_base
UNION ALL
SELECT
  fecha,
  'global' AS nivel,
  tipo_movimiento_cod,
  '0000' AS cod_pais,
  unidades,
  valor,
  'original' AS modelo
FROM taric_global
UNION ALL
SELECT
  fecha,
  nivel,
  tipo_movimiento_cod,
  cod_pais,
  unidades,
  valor,
  modelo
FROM taric_general_forecast