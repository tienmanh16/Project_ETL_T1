


select * from {{ source('SALES_SOURCE_STAGE','kpi_table') }}
