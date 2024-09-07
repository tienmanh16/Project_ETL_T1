{{
    config(
        materialized='incremental',
        unique_key='ten_chi_nhanh'
    )
}}

WITH base AS (
    SELECT 
        c.ten_chi_nhanh,
        SUM(d.doanh_thu) as tong_doanh_thu,
        d.last_updated
    FROM 
        {{ source('SALES_SOURCE_ANALYST','du_lieu_ban_hang_table') }} d
    JOIN 
        {{ source('SALES_SOURCE_ANALYST','chi_nhanh_table') }} c
    ON 
        d.ma_chi_nhanh = c.ma_chi_nhanh

    {% if is_incremental() %}
      -- Filter chỉ áp dụng trong trường hợp chạy incremental
      WHERE d.last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
    {% endif %}

    GROUP BY 
        c.ten_chi_nhanh,
        d.last_updated
)

SELECT * FROM base;
