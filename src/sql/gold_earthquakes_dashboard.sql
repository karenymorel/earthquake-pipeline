CREATE OR REPLACE TABLE main.gold_earthquakes_dashboard AS
SELECT 
    id,
    place,
    CAST(magnitude AS DOUBLE) AS magnitude,
    CAST(depth AS DOUBLE) AS depth,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CONCAT(CAST(latitude AS VARCHAR), ',', CAST(longitude AS VARCHAR)) AS coordenadas,
    CASE 
        WHEN magnitude >= 6.0 THEN 'Alto'
        WHEN magnitude >= 4.5 THEN 'Moderado'
        ELSE 'Bajo'
    END AS categoria_intensidad
FROM main.silver_earthquakes;