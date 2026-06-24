-- Mart: price statistics aggregated by flat type.

SELECT
    flat_type,
    COUNT(*)                         AS transaction_count,
    ROUND(MEDIAN(resale_price), 0)   AS median_price,
    ROUND(AVG(resale_price), 0)      AS avg_price,
    ROUND(AVG(floor_area_sqm), 1)    AS avg_floor_area_sqm,
    ROUND(MEDIAN(price_per_sqm), 0)  AS median_price_per_sqm,
    ROUND(AVG(flat_age), 1)          AS avg_flat_age_years
FROM clean_hdb_resale
GROUP BY flat_type
ORDER BY avg_floor_area_sqm
