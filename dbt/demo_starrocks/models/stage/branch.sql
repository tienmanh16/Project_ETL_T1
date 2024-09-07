

select * from {{ source('SALES_SOURCE_STAGE','chi_nhanh_table') }}
