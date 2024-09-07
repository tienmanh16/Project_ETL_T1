{{ config(
    materialized='table',
    replace=True
) }}

SELECT 
    TO_CHAR(NGAY_HACH_TOAN, 'YYYY-MM') AS THANG,
    CHI_NHANH,
    SUM(DOANH_THU) AS TONG_DOANH_THU
FROM 
    {{ source('BAI_TEST_SOURCE','DU_LIEU_BAN_HANG_TABLE') }}
GROUP BY 
    TO_CHAR(NGAY_HACH_TOAN, 'YYYY-MM'), 
    CHI_NHANH
ORDER BY 
    THANG

