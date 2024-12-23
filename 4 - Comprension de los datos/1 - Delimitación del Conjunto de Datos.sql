-- Extraer el codigo de pais para cada uno de los miembros de la Comunidad Andina.

SELECT cod_pais, pais 
FROM `unir-predictiv0-andina-espana.datacomex.paises` 
WHERE pais LIKE 'Boli%' 
   OR pais LIKE 'Colom%' 
   OR pais LIKE 'Ecua%' 
   OR pais LIKE 'Per%';

-- Verificar la existencia de registros para los paises de la Comunidad Andina en la tabla de Comercio Exterior.

SELECT 
    p.pais AS nombre_pais,
    DATE(c.anio, c.mes, 1) AS fecha,
    SUM(CASE WHEN c.tipo_movimiento = 'I' THEN c.euros ELSE 0 END) AS valor_importacion_euros,
    SUM(CASE WHEN c.tipo_movimiento = 'E' THEN c.euros ELSE 0 END) AS valor_exportacion_euros
FROM 
    `unir-predictiv0-andina-espana.datacomex.comex` c
JOIN 
    `unir-predictiv0-andina-espana.datacomex.paises` p
ON 
    c.cod_pais = p.cod_pais
WHERE 
    c.cod_pais IN ('504', '516', '500', '480')
GROUP BY 
    p.pais, fecha
ORDER BY 
    p.pais, fecha;


-- Crear una tabla con los registros de comercio exterior para los paises de la Comunidad Andina para perfilado. 

CREATE TABLE `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina` AS
SELECT
    c.tipo_movimiento,
    c.cod_pais,
    p.pais AS nombre_pais,
    c.cod_provincia,
    pr.nombre_provincia,
    pr.nombre_comunidad,
    c.estado,
    CAST(c.euros AS FLOAT64) AS euros,
    CAST(c.dolares AS FLOAT64) AS dolares,
    c.nivel_taric_original AS comex_nivel_taric,
    c.cod_taric,
    CAST(c.kilogramos AS FLOAT64) AS kilogramos,
    c.anio,
    c.mes,
    ct.nivel_taric AS nivel_taric_detalle,
    ct.descripcion_taric
FROM
    `unir-predictiv0-andina-espana.datacomex.comex` c
    JOIN `unir-predictiv0-andina-espana.datacomex.paises` p ON c.cod_pais = p.cod_pais
    JOIN `unir-predictiv0-andina-espana.datacomex.provincias` pr ON c.cod_provincia = pr.cod_provincia
    JOIN `unir-predictiv0-andina-espana.datacomex.clasificacion_taric` ct ON c.cod_taric = ct.cod_taric
WHERE
    c.cod_pais IN ('504', '516', '500', '480');



-- Consulta de comunidades No identificadas 

SELECT 
    DATE(anio, mes, 1) AS fecha,
    COUNT(CASE WHEN nombre_comunidad = 'No determinado' THEN 1 END) AS conteo_no_determinado,
    COUNT(*) AS total_registros,
    ROUND(COUNT(CASE WHEN nombre_comunidad = 'No determinado' THEN 1 END) / COUNT(*), 2) AS porcentaje_no_determinado
FROM 
    `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`
GROUP BY 
    fecha
ORDER BY 
    fecha;
