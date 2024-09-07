
select * from {{ source('SALES_SOURCE_STAGE','du_lieu_ban_hang_table') }}
