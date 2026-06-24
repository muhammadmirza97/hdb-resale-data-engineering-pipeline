-- Mart: price statistics aggregated by town.
-- Reads from the clean_hdb_resale table (created by src/transform.py).

SELECT
    town,
    COUNT(*)                         AS transaction_count,
    ROUND(MEDIAN(resale_price), 0)   AS median_price,
    ROUND(AVG(resale_price), 0)      AS avg_price,
    ROUND(MIN(resale_price), 0)      AS min_price,
    ROUND(MAX(resale_price), 0)      AS max_price,
    ROUND(AVG(floor_area_sqm), 1)    AS avg_floor_area_sqm,
    ROUND(MEDIAN(price_per_sqm), 0)  AS median_price_per_sqm,
    MIN(sale_year)                   AS first_year,
    MAX(sale_year)                   AS last_year
FROM clean_hdb_resale
GROUP BY town
ORDER BY median_price DESC
