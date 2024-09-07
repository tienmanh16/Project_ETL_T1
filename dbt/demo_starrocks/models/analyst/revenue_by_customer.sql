{{
    config(
        materialized='incremental',
        unique_key='ma_kh'
    )
}}


WITH base AS (
SELECT 
    kh.ma_kh,
    kh.Khach_hang,
    SUM(bh.doanh_thu) AS tong_doanh_thu,
    bh.last_updated
FROM 
    {{ source('SALES_SOURCE_ANALYST','khach_hang_table') }} kh
JOIN 
    {{ source('SALES_SOURCE_ANALYST','du_lieu_ban_hang_table') }} bh 
    ON kh.ma_kh = bh.ma_kh
GROUP BY 
    kh.ma_kh,
    kh.khach_hang,
    bh.last_updated
ORDER BY 
    tong_doanh_thu DESC
)

SELECT * FROM base

{% if is_incremental() %}
  -- Filter chỉ áp dụng trong trường hợp chạy incremental
  WHERE last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}
