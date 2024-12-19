SELECT COUNT(1) as conteo, comex_nivel_taric, nivel_taric_detalle
FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`
GROUP BY comex_nivel_taric, nivel_taric_detalle;



SELECT distinct cod_taric
FROM `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`


SELECT 
    nivel_taric_detalle,
    SUM(dolares) AS suma_total_dolares,
    SUM(euros) AS suma_total_euros
    
FROM 
    `unir-predictiv0-andina-espana.datacomex.comex_comunidad_andina`
GROUP BY 
    nivel_taric_detalle