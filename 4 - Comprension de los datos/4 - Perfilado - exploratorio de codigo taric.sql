-- Perfilado de campo nivel_taric_detalle

SELECT COUNT(1) as conteo, comex_nivel_taric, nivel_taric_detalle
FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`
GROUP BY comex_nivel_taric, nivel_taric_detalle;

-- Consulta para detectar comportamiento de los campos nivel_taric respecto a dolares y euros
SELECT 
    nivel_taric_detalle,
    SUM(dolares) AS suma_total_dolares,
    SUM(euros) AS suma_total_euros
    
FROM 
    `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`
GROUP BY 
    nivel_taric_detalle