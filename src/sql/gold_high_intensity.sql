CREATE OR REPLACE TABLE gold_high_intensity_events AS
SELECT 
    id,
    place,
    magnitude,
    depth,
    latitude,
    longitude,
    timestamp
FROM silver_earthquakes
WHERE magnitude >= 2.5
ORDER BY magnitude DESC;