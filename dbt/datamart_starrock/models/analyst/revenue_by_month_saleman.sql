{{ config(
    materialized='table'
) }}

WITH base AS (
SELECT 
    DATE_FORMAT(ngay_hach_toan, '%Y-%m') AS thang,
    NV.nhan_vien_ban,
    SUM(doanh_thu) AS tong_doanh_thu,
    DL.last_updated
FROM 
    {{ source('SALES_SOURCE','nhan_vien_table') }} NV
JOIN 
    {{ source('SALES_SOURCE','du_lieu_ban_hang_table') }} DL
ON
    NV.ma_nhan_vien_ban = DL.ma_nhan_vien_ban 
GROUP BY 
    DATE_FORMAT(ngay_hach_toan, '%Y-%m'),
    NV.nhan_vien_ban,
    DL.last_updated
ORDER BY 
    thang
)

SELECT * FROM base


