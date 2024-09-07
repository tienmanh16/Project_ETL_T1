{{ config(
    materialized='table'
) }}

SELECT 
    kh.Ma_KH,
    kh.Khach_hang,
    SUM(bh.Doanh_thu) AS Tong_doanh_thu
FROM 
    BAI_TEST.KHACH_HANG_TABLE kh
JOIN 
    BAI_TEST.DU_LIEU_BAN_HANG_TABLE bh 
    ON kh.Ma_KH = bh.Ma_KH
GROUP BY 
    kh.Ma_KH,
    kh.Khach_hang
ORDER BY 
    Tong_doanh_thu DESC