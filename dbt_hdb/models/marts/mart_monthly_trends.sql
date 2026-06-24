-- Mart: monthly price trends — used for time-series visualisations.

SELECT
    sale_year,
    sale_month,
    DATE_TRUNC('month', STRPTIME(month, '%Y-%m')) AS period,
    COUNT(*)                                       AS transaction_count,
    ROUND(MEDIAN(resale_price), 0)                 AS median_price,
    ROUND(AVG(resale_price), 0)                    AS avg_price,
    ROUND(MEDIAN(price_per_sqm), 0)                AS median_price_per_sqm
FROM clean_hdb_resale
GROUP BY sale_year, sale_month, period
ORDER BY period
