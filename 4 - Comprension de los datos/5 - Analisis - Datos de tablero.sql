CREATE TABLE `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina_tablero` AS
with comex as (
    SELECT
        DATE(anio, mes, 1) AS fecha,
        CASE
            WHEN tipo_movimiento = 'I' THEN 'Importación'
            WHEN tipo_movimiento = 'E' THEN 'Exportación'
            ELSE 'No determinado'
        END AS tipo_movimiento,
        nombre_pais,
        nombre_provincia,
        nombre_comunidad,
        estado,
        dolares,
        kilogramos,
        SUBSTR(cod_taric, 1, 2) AS cod_taric_1,
        SUBSTR(cod_taric, 1, 4) AS cod_taric_2,
        SUBSTR(cod_taric, 1, 6) AS cod_taric_3,
        SUBSTR(cod_taric, 1, 8) AS cod_taric_4,
        cod_taric AS cod_taric_5,
        descripcion_taric as taric_desc_5
    FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`
    WHERE comex_nivel_taric = '5'
)
select
    comex.fecha,
    comex.tipo_movimiento,
    comex.nombre_pais,
    comex.nombre_provincia,
    comex.nombre_comunidad,
    comex.estado,
    comex.dolares,
    comex.kilogramos,
    comex.cod_taric_1,
    REGEXP_REPLACE(RTRIM(TRIM(REGEXP_REPLACE(desc_lvl1.descripcion_taric, r'^[_\s]+', '')), ':'), r'::+$', '') AS taric_desc_1,
    comex.cod_taric_2,
    REGEXP_REPLACE(RTRIM(TRIM(REGEXP_REPLACE(desc_lvl2.descripcion_taric, r'^[_\s]+', '')), ':'), r'::+$', '') AS taric_desc_2,
    comex.cod_taric_3,
    REGEXP_REPLACE(RTRIM(TRIM(REGEXP_REPLACE(desc_lvl3.descripcion_taric, r'^[_\s]+', '')), ':'), r'::+$', '') AS taric_desc_3,
    comex.cod_taric_4,
    REGEXP_REPLACE(RTRIM(TRIM(REGEXP_REPLACE(desc_lvl4.descripcion_taric, r'^[_\s]+', '')), ':'), r'::+$', '') AS taric_desc_4,
    comex.cod_taric_5,
    REGEXP_REPLACE(RTRIM(TRIM(REGEXP_REPLACE(comex.taric_desc_5, r'^[_\s]+', '')), ':'), r'::+$', '') AS taric_desc_5
from
    comex
    LEFT JOIN `unir-predictiv0-andina-espana.datacomex.clasificacion_taric` AS desc_lvl1 
        ON comex.cod_taric_1 = desc_lvl1.cod_taric
    LEFT JOIN `unir-predictiv0-andina-espana.datacomex.clasificacion_taric` AS desc_lvl2 
        ON comex.cod_taric_2 = desc_lvl2.cod_taric
    LEFT JOIN `unir-predictiv0-andina-espana.datacomex.clasificacion_taric` AS desc_lvl3 
        ON comex.cod_taric_3 = desc_lvl3.cod_taric
    LEFT JOIN `unir-predictiv0-andina-espana.datacomex.clasificacion_taric` AS desc_lvl4 
        ON comex.cod_taric_4 = desc_lvl4.cod_taric