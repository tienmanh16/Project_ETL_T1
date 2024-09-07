{{ config(
    materialized='table'
) }}


WITH base AS (
SELECT 
    DATE_FORMAT(ngay_hach_toan, '%Y-%m') AS thang,
    ma_chi_nhanh,
    SUM(doanh_thu) AS tong_doanh_thu,
    DL.last_updated
FROM 
    {{ source('SALES_SOURCE','du_lieu_ban_hang_table') }} DL
GROUP BY 
    DATE_FORMAT(ngay_hach_toan, '%Y-%m'), 
    ma_chi_nhanh,
    DL.last_updated
ORDER BY 
    thang
)

SELECT * FROM base



