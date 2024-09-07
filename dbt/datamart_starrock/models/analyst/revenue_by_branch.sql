{{ config(
    materialized='table'
) }}

    SELECT 
        c.ten_chi_nhanh,
        SUM(d.doanh_thu) as tong_doanh_thu,
        d.last_updated
    FROM 
        {{ source('SALES_SOURCE','du_lieu_ban_hang_table') }} d
    JOIN 
        {{ source('SALES_SOURCE','chi_nhanh_table') }} c
    ON 
        d.ma_chi_nhanh = c.ma_chi_nhanh
    GROUP BY 
        c.ten_chi_nhanh,
        d.last_updated
