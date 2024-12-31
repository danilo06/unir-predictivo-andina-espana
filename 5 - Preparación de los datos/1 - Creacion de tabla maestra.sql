CREATE TABLE `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_modelo` AS 
WITH taric AS (
    SELECT
        subquery.cod_taric,
        subquery.nivel_taric,
        subquery.descripcion_taric_original,
        CASE
            WHEN subquery.descripcion_taric = 'Las demás' THEN 'Los demás'
            ELSE CONCAT(
                UPPER(SUBSTR(subquery.descripcion_taric, 1, 1)),
                LOWER(SUBSTR(subquery.descripcion_taric, 2))
            )
        END AS descripcion_taric
    FROM
        (
            SELECT
                clasificacion.cod_taric,
                clasificacion.nivel_taric,
                clasificacion.descripcion_taric AS descripcion_taric_original,
                TRIM(
                    REPLACE(
                        REPLACE(
                            REPLACE(clasificacion.descripcion_taric, '_', ''),
                            '.',
                            ''
                        ),
                        ':',
                        ''
                    )
                ) AS descripcion_taric
            FROM
                `unir-predictiv0-andina-espana.datacomex.clasificacion_taric` clasificacion
        ) AS subquery
),
comex as (
    SELECT
        DATE(comex.anio, comex.mes, 1) AS fecha,
        comex.anio,
        comex.mes,
        tipo_movimiento AS tipo_movimiento_cod,
        CASE
            WHEN comex.tipo_movimiento = 'I' THEN 'Importacion'
            WHEN comex.tipo_movimiento = 'E' THEN 'Exportacion'
        END AS tipo_movimiento,
        comex.estado AS estado_transaccion,
        comex.cod_pais,
        comex.cod_provincia,
        comex.nivel_taric_original AS nivel_taric,
        comex.cod_taric,
        CAST(comex.euros AS FLOAT64) AS euros,
        CAST(comex.dolares AS FLOAT64) AS dolares,
        CAST(comex.kilogramos AS FLOAT64) AS kilogramos
    FROM
        `unir-predictiv0-andina-espana.datacomex.comex` comex
    WHERE
        comex.cod_pais IN ('504', '516', '500', '480')
)
SELECT
    comex.fecha,
    comex.anio,
    comex.mes,
    comex.tipo_movimiento_cod,
    comex.tipo_movimiento,
    comex.estado_transaccion,
    comex.cod_pais,
    paises.pais as nombre_pais,
    comex.cod_provincia,
    provincias.nombre_provincia,
    provincias.cod_comunidad,
    provincias.nombre_comunidad,
    comex.nivel_taric,
    comex.cod_taric,
    taric.descripcion_taric,
    comex.euros,
    comex.dolares,
    comex.kilogramos
FROM
    comex
    LEFT JOIN taric ON comex.cod_taric = taric.cod_taric
    LEFT JOIN `unir-predictiv0-andina-espana.datacomex.paises` paises ON comex.cod_pais = paises.cod_pais
    LEFT JOIN `unir-predictiv0-andina-espana.datacomex.provincias` provincias ON comex.cod_provincia = provincias.cod_provincia