{{
    config(
        materialized = 'incremental',
        unique_key = ['thang', 'ma_chi_nhanh'],
    )
}}

WITH base AS (
    SELECT 
        DATE_FORMAT(ngay_hach_toan, '%Y-%m') AS thang,
        ma_chi_nhanh,
        SUM(doanh_thu) AS tong_doanh_thu,
        DL.last_updated
    FROM 
        {{ source('SALES_SOURCE_ANALYST', 'du_lieu_ban_hang_table') }} DL

    {% if is_incremental() %}
      -- Filter chỉ áp dụng trong trường hợp chạy incremental
      WHERE DL.last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}

    GROUP BY 
        DATE_FORMAT(ngay_hach_toan, '%Y-%m'), 
        ma_chi_nhanh,
        DL.last_updated
)

SELECT * FROM base
ORDER BY thang;
