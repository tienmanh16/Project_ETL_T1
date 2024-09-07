{{
    config(
        materialized='incremental',
        unique_key='TEN_CHI_NHANH'
    )
}}

WITH base AS (
    SELECT 
        c.TEN_CHI_NHANH,
        SUM(d.DOANH_THU) AS TONG_DOANH_THU,
        d.last_updated
    FROM 
        {{ source('SALES_SOURCE','DU_LIEU_BAN_HANG_TABLE') }} d
    JOIN 
        {{ source('SALES_SOURCE','CHI_NHANH_TABLE') }} c
    ON 
        d.MA_CHI_NHANH = c.MA_CHI_NHANH
    GROUP BY 
        c.TEN_CHI_NHANH,
        d.last_updated
)

SELECT * FROM base

{% if is_incremental() %}
  -- Filter chỉ áp dụng trong trường hợp chạy incremental
  WHERE last_updated >= (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}


--phải tạo trường last_update để đánh dấu mốc tgian
--để chạy dc incremental thì phải tạo bảng trong db đã
---xong mới setup config incremental và macro 
