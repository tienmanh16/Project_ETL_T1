{{
    config(
        materialized = 'incremental',
        unique_key = ['thang', 'nhan_vien_ban']
    )
}}

WITH base AS (
    SELECT 
        DATE_FORMAT(ngay_hach_toan, '%Y-%m') AS thang,
        NV.nhan_vien_ban,
        SUM(doanh_thu) AS tong_doanh_thu,
        DL.last_updated
    FROM 
        {{ source('SALES_SOURCE_ANALYST','nhan_vien_table') }} NV
    JOIN 
        {{ source('SALES_SOURCE_ANALYST','du_lieu_ban_hang_table') }} DL
    ON
        NV.ma_nhan_vien_ban = DL.ma_nhan_vien_ban 

    {% if is_incremental() %}
      WHERE DL.last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}

    GROUP BY 
        DATE_FORMAT(ngay_hach_toan, '%Y-%m'),
        NV.nhan_vien_ban,
        DL.last_updated
)

SELECT * FROM base
ORDER BY thang;
