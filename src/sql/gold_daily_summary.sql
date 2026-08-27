CREATE OR REPLACE TABLE gold_daily_summary AS
SELECT 
    CAST(timestamp AS DATE) AS event_date,
    COUNT(*) AS total_earthquakes,
    ROUND(AVG(magnitude), 2) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude,
    ROUND(AVG(depth), 2) AS avg_depth_km
FROM silver_earthquakes
GROUP BY 1;