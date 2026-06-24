-- Staging: deduplicate and validate raw HDB resale data.
-- Reads from raw_hdb_resale (loaded by src/ingest.py).
-- Output used by downstream mart models.

SELECT DISTINCT
    month,
    town,
    flat_type,
    block,
    street_name,
    storey_range,
    CAST(floor_area_sqm AS DOUBLE)       AS floor_area_sqm,
    flat_model,
    CAST(lease_commence_date AS INTEGER) AS lease_commence_date,
    remaining_lease,
    CAST(resale_price AS DOUBLE)         AS resale_price
FROM raw_hdb_resale
WHERE
    resale_price   IS NOT NULL AND CAST(resale_price AS DOUBLE) > 0
    AND floor_area_sqm IS NOT NULL AND CAST(floor_area_sqm AS DOUBLE) > 0
    AND month      IS NOT NULL
    AND storey_range   IS NOT NULL
    AND remaining_lease IS NOT NULL
    AND lease_commence_date IS NOT NULL
