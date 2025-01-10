
CREATE TABLE `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo_tablero` AS 
with taric_base as (
  SELECT
    fecha,
    tipo_movimiento_cod,
    cod_pais,
    SUM(dolares) AS dolares,
    SUM(kilogramos) AS kilogramos
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo`
  WHERE nivel_taric = '1'
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
taric_general_forecast AS (
  SELECT
    dlrs.fecha,
    CASE
        WHEN dlrs.llave3 = 'GLOBAL' THEN 'global'
        ELSE 'pais'
    END AS nivel,
    dlrs.llave2 AS tipo_movimiento_cod,
    CASE
      WHEN dlrs.llave3 = 'GLOBAL' THEN '0000'
      ELSE dlrs.llave3
    END AS cod_pais,
    dlrs.prediccion as dolares,
    klgrms.prediccion as kilogramos,
    dlrs.modelo
  FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_forecast` as dlrs
  INNER JOIN `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_forecast` as klgrms
    ON dlrs.FORECAST_LEVEL = klgrms.FORECAST_LEVEL
      AND dlrs.llave2 = klgrms.llave2
      AND dlrs.llave3 = klgrms.llave3
      AND dlrs.modelo = klgrms.modelo
  WHERE dlrs.llave2 <> 'producto'
    AND dlrs.llave1 = 'dolares'
    AND klgrms.llave1 = 'kilogramos'
)
SELECT
  fecha,
  'pais' AS nivel,
  tipo_movimiento_cod,
  cod_pais,
  dolares,
  kilogramos,
  'original' AS modelo
FROM taric_base
UNION ALL
SELECT
  fecha,
  'global' AS nivel,
  tipo_movimiento_cod,
  '0000' AS cod_pais,
  dolares,
  kilogramos,
  'original' AS modelo
FROM taric_global
UNION ALL
SELECT
  fecha,
  nivel,
  tipo_movimiento_cod,
  cod_pais,
  dolares,
  kilogramos,
  modelo
FROM taric_general_forecast

# Filtro dinamico para tomar parametro de referencia 

CASE 
  WHEN (Modelo = 'ARIMA' AND modelo_org = 'ARIMA') OR (modelo_org = 'original') THEN 'Mostrar'
  WHEN (Modelo = 'MLP' AND modelo_org = 'MLP') OR (modelo_org = 'original') THEN 'Mostrar'
  WHEN (Modelo = 'LSTM' AND modelo_org = 'LSTM') OR (modelo_org = 'original') THEN 'Mostrar'
  ELSE 'Ocultar'
END
