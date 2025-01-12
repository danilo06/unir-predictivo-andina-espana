
CREATE TABLE `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo_tablero` AS 
with taric_base as (
  SELECT
    fecha,
    tipo_movimiento_cod,
    cod_pais,
    cod_taric,
    SUM(dolares) AS dolares,
    SUM(kilogramos) AS kilogramos
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo`
  WHERE nivel_taric = '1'
  GROUP BY 1, 2, 3, 4
),
taric_productos AS (
  SELECT
    fecha,
    cod_taric,
    SUM(dolares) AS dolares,
    SUM(kilogramos) AS kilogramos
  FROM taric_base
  GROUP BY 1, 2
),
taric_pais as (
  SELECT
    fecha,
    tipo_movimiento_cod,
    cod_pais,
    SUM(dolares) AS dolares,
    SUM(kilogramos) AS kilogramos
  FROM taric_base
  GROUP BY 1, 2, 3
),
taric_global AS (
  SELECT
    fecha,
    tipo_movimiento_cod,
    SUM(dolares) AS dolares,
    SUM(kilogramos) AS kilogramos
  FROM taric_base
  GROUP BY 1, 2
),
mape_ranked_predictions AS (
  SELECT
    Producto,
    modelo,
    prediccion as mape,
    ROW_NUMBER() OVER (
      PARTITION BY Producto
      ORDER BY CAST(prediccion AS FLOAT64) ASC
    ) AS ranking_modelo
  FROM
    `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_mape`
),
taric_forecast AS (
  SELECT
      dlrs.fecha,
      CASE
          WHEN dlrs.llave2 = 'producto' THEN 'PRODUCTO'
          WHEN dlrs.llave3 = 'GLOBAL' THEN 'GLOBAL'
          ELSE 'PAIS'
      END AS nivel,
        CASE
          WHEN dlrs.llave2 = 'producto' THEN 'T'
          ELSE dlrs.llave2
      END AS tipo_movimiento_cod,
      CASE
        WHEN (dlrs.llave2 = 'producto' OR dlrs.llave3 = 'GLOBAL') THEN '000'
        ELSE dlrs.llave3
      END AS cod_pais,
      CASE
        WHEN dlrs.llave2 = 'producto' THEN dlrs.llave3
        ELSE '00'
      END AS cod_taric,
      dlrs.prediccion as dolares,
      klgrms.prediccion as kilogramos,
      dlrs.modelo,
      mrp_dolares.ranking_modelo as dolares_ranking_modelo,
      mrp_dolares.mape as dolares_mape_modelo,
      mrp_kilogramos.ranking_modelo as kilogramos_ranking_modelo,
      mrp_kilogramos.mape as kilogramos_mape_modelo
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_forecast` dlrs
  INNER JOIN `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_forecast` klgrms
    ON dlrs.FORECAST_LEVEL = klgrms.FORECAST_LEVEL
      AND dlrs.llave2 = klgrms.llave2
      AND dlrs.llave3 = klgrms.llave3
      AND dlrs.modelo = klgrms.modelo
  LEFT JOIN mape_ranked_predictions AS mrp_dolares
  ON dlrs.Producto = mrp_dolares.Producto AND dlrs.modelo = mrp_dolares.modelo
  LEFT JOIN mape_ranked_predictions AS mrp_kilogramos
  ON klgrms.Producto = mrp_kilogramos.Producto AND klgrms.modelo = mrp_kilogramos.modelo
  WHERE dlrs.llave1 = 'dolares'
      AND klgrms.llave1 = 'kilogramos'
      AND dlrs.llave2 <> 'T'
),
 taric_limpio as (
  SELECT DISTINCT
    cod_taric,
    descripcion_taric
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo`
  WHERE nivel_taric = '1'
),
all_taric as (
    SELECT
    fecha,
    'PRODUCTO' AS nivel,
    'T' AS tipo_movimiento_cod,
    '000' AS cod_pais,
    cod_taric,
    dolares,
    kilogramos,
    'ORIGINAL' AS modelo
    ,NULL AS dolares_ranking_modelo
    ,NULL AS dolares_mape_modelo
    ,NULL AS kilogramos_ranking_modelo
    ,NULL AS kilogramos_mape_modelo
  FROM taric_productos
  UNION ALL
  SELECT
    fecha,
    'PAIS' AS nivel,
    tipo_movimiento_cod,
    cod_pais,
    '00' as cod_taric,
    dolares,
    kilogramos,
    'ORIGINAL' AS modelo
    ,NULL AS dolares_ranking_modelo
    ,NULL AS dolares_mape_modelo
    ,NULL AS kilogramos_ranking_modelo
    ,NULL AS kilogramos_mape_modelo
  FROM taric_pais
  UNION ALL
  SELECT
    fecha,
    'GLOBAL' AS nivel,
    tipo_movimiento_cod,
    '000' AS cod_pais,
    '00' as cod_taric,
    dolares,
    kilogramos,
    'ORIGINAL' AS modelo
    ,NULL AS dolares_ranking_modelo
    ,NULL AS dolares_mape_modelo
    ,NULL AS kilogramos_ranking_modelo
    ,NULL AS kilogramos_mape_modelo
  FROM taric_global
  UNION ALL
  SELECT
    fecha,
    nivel,
    tipo_movimiento_cod,
    cod_pais,
    cod_taric,
    dolares,
    kilogramos,
    modelo
    ,dolares_ranking_modelo
    ,dolares_mape_modelo
    ,kilogramos_ranking_modelo
    ,kilogramos_mape_modelo
  FROM taric_forecast
)
SELECT 
  atr.fecha,
  atr.nivel,
  atr.tipo_movimiento_cod,
  CASE 
    WHEN atr.tipo_movimiento_cod = 'I' THEN 'IMPORTACION'
    WHEN atr.tipo_movimiento_cod = 'E' THEN 'EXPORTACION'
    WHEN atr.tipo_movimiento_cod = 'T' THEN 'TOTAL'
    ELSE atr.tipo_movimiento_cod
  END AS tipo_movimiento,
  atr.cod_pais,
  CASE 
    WHEN atr.cod_pais = '504' THEN 'PERÚ'
    WHEN atr.cod_pais = '516' THEN 'BOLIVIA'
    WHEN atr.cod_pais = '500' THEN 'ECUADOR'
    WHEN atr.cod_pais = '480' THEN 'COLOMBIA'
    ELSE 'GLOBAL'
  END AS pais,
  atr.cod_taric,
  COALESCE(tl.descripcion_taric, 'Global') AS descripcion_taric,
  atr.dolares,
  atr.kilogramos,
  atr.modelo
  ,dolares_ranking_modelo
  ,dolares_mape_modelo
  ,kilogramos_ranking_modelo
  ,kilogramos_mape_modelo
FROM all_taric atr
LEFT JOIN taric_limpio tl USING (cod_taric)
