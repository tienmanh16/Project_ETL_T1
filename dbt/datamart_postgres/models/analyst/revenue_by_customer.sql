{{
    config(
        materialized='incremental',
        unique_key='Ma_KH'
    )
}}

WITH base AS (
SELECT 
    kh.Ma_KH,
    kh.Khach_hang,
    SUM(bh.Doanh_thu) AS Tong_doanh_thu,
    bh.last_updated
FROM 
    {{ source('SALES_SOURCE','KHACH_HANG_TABLE') }} kh
JOIN 
    {{ source('SALES_SOURCE','DU_LIEU_BAN_HANG_TABLE') }} bh 
    ON kh.Ma_KH = bh.Ma_KH
GROUP BY 
    kh.Ma_KH,
    kh.Khach_hang,
    bh.last_updated
ORDER BY 
    Tong_doanh_thu DESC
)

SELECT * FROM base

{% if is_incremental() %}
  -- Filter chỉ áp dụng trong trường hợp chạy incremental
  WHERE last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}
