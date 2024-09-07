{{
    config(
        materialized='incremental',
        unique_key=['THANG','MA_CHI_NHANH'],
    )
}}

WITH base AS (
SELECT 
    TO_CHAR(NGAY_HACH_TOAN, 'YYYY-MM') AS THANG,
    MA_CHI_NHANH,
    SUM(DOANH_THU) AS TONG_DOANH_THU,
    DL.last_updated
FROM 
    {{ source('SALES_SOURCE','DU_LIEU_BAN_HANG_TABLE') }} DL
GROUP BY 
    TO_CHAR(NGAY_HACH_TOAN, 'YYYY-MM'), 
    MA_CHI_NHANH,
    DL.last_updated
ORDER BY 
    THANG

)

SELECT * FROM base

{% if is_incremental() %}
  -- Filter chỉ áp dụng trong trường hợp chạy incremental
  WHERE last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}

