{{
    config(
        materialized='incremental',
        unique_key=['NHAN_VIEN_BAN','THANG']
    )
}}
    
WITH base AS (
SELECT 
    TO_CHAR(NGAY_HACH_TOAN, 'YYYY-MM') AS THANG,
    NV.NHAN_VIEN_BAN,
    SUM(DOANH_THU) AS TONG_DOANH_THU,
    DL.last_updated
FROM 
    {{ source('SALES_SOURCE','NHAN_VIEN_TABLE') }} NV
JOIN 
    {{ source('SALES_SOURCE','DU_LIEU_BAN_HANG_TABLE') }} DL
ON
    NV.MA_NHAN_VIEN_BAN = DL.MA_NHAN_VIEN_BAN
GROUP BY 
    TO_CHAR(NGAY_HACH_TOAN, 'YYYY-MM'),
    NV.NHAN_VIEN_BAN,
    DL.last_updated
ORDER BY 
    THANG
)

SELECT * FROM base


{% if is_incremental() %}
  -- Filter chỉ áp dụng trong trường hợp chạy incremental
  WHERE last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}