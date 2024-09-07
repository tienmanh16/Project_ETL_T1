{{ config(
    materialized='table'
) }}


WITH base AS (
SELECT 
    kh.ma_kh,
    kh.Khach_hang,
    SUM(bh.doanh_thu) AS tong_doanh_thu,
    bh.last_updated
FROM 
    {{ source('SALES_SOURCE','khach_hang_table') }} kh
JOIN 
    {{ source('SALES_SOURCE','du_lieu_ban_hang_table') }} bh 
    ON kh.ma_kh = bh.ma_kh
GROUP BY 
    kh.ma_kh,
    kh.khach_hang,
    bh.last_updated
ORDER BY 
    tong_doanh_thu DESC
)

SELECT * FROM base

