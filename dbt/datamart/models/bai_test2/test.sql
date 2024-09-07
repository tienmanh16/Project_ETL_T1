{{ config(
    materialized='table',
    replace=True
) }}

SELECT * FROM BAI_TEST.CHI_NHANH_TABLE
