from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
import pandas as pd
import oracledb
from plugins.config import config
from datetime import timedelta
from datetime import datetime
import pendulum
import cx_Oracle
from sqlalchemy import types, create_engine
import re
import psycopg2
from plugins.minio_helper import MinioHelper
from datetime import datetime
import pymysql

minio = MinioHelper()
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")




def starrocks_connection():
    connection = pymysql.connect(
        host=config.DB_HOST,
        port=9030,
        user=config.DB_STARROCKS_USER,
        password=config.DB_STARROCKS_PASSWORD,
        database=config.DB_STARROCKS_SCHEMA,
    )
    return connection

# def starrocks_connection():
#     connection = pymysql.connect(
#         host='host.docker.internal',
#         port=9030,
#         user='tien',
#         password='tien',
#         database='db_starrocks',
#     )
#     return connection

files = [
    {"name":"KPI_Template.xlsx"},
    {"name":"Modeling_and_DAX.xlsx"}
] 

def upload_file_to_minio():
    date = datetime.today().date()
    for item in files:
        with open(f'/opt/airflow/data/{item.get("name")}', 'rb') as file:
            # Ghi tệp lên MinIO
            minio.upload_xlsx_to_minio(file, object_name=f'raw_data/{date}/{item.get("name")}')
def get_last_run_time(cursor):
    cursor.execute("SELECT MAX(last_updated) FROM chi_nhanh_table")
    last_run_time = cursor.fetchone()[0]
    return last_run_time if last_run_time else datetime(1900, 1, 1)
def import_excel_to_starrocks():
    date = datetime.today().date()
    kpi_file = minio.read_xlsx(object_name=f'raw_data/{date}/KPI_Template.xlsx')
    modeling_dax_file = minio.read_xlsx(object_name=f'raw_data/{date}/Modeling_and_DAX.xlsx')

    # Read file KPI Template
    kpi_df = pd.read_excel(kpi_file, sheet_name='KPI theo năm')
    kpi_df['KPI'] = kpi_df['KPI'].str.replace(',', '', regex=False).astype(int)
    kpi_df['last_updated'] = datetime.now() 
    # print(kpi_df.head())
    # print(kpi_df.dtypes)
    # Reads sheets in Modeling and DAX file
    khach_hang_df = pd.read_excel(modeling_dax_file, sheet_name='Khách hàng')
    san_pham_df = pd.read_excel(modeling_dax_file, sheet_name='Sản phẩm')
    nhan_vien_df = pd.read_excel(modeling_dax_file, sheet_name='Nhân viên')
    du_lieu_ban_hang_df = pd.read_excel(modeling_dax_file, sheet_name='Dữ liệu bán hàng')
    chi_nhanh_df = pd.read_excel(modeling_dax_file, sheet_name='Chi nhánh')

    # Add last_updated column
    khach_hang_df['last_updated'] = pd.to_datetime('now')
    san_pham_df['last_updated'] = pd.to_datetime('now')
    nhan_vien_df['last_updated'] = pd.to_datetime('now')
    du_lieu_ban_hang_df['last_updated'] = pd.to_datetime('now')
    chi_nhanh_df['last_updated'] = pd.to_datetime('now')

    conn = starrocks_connection()
    cursor = conn.cursor()

    last_run_time = get_last_run_time(cursor)

    # Filter data based on last_updated
    kpi_df = kpi_df[kpi_df['last_updated'] > last_run_time]
    khach_hang_df = khach_hang_df[khach_hang_df['last_updated'] > last_run_time]
    san_pham_df = san_pham_df[san_pham_df['last_updated'] > last_run_time]
    nhan_vien_df = nhan_vien_df[nhan_vien_df['last_updated'] > last_run_time]
    du_lieu_ban_hang_df = du_lieu_ban_hang_df[du_lieu_ban_hang_df['last_updated'] > last_run_time]
    chi_nhanh_df = chi_nhanh_df[chi_nhanh_df['last_updated'] > last_run_time]

    # Insert or update data to Starrocks
    # Ví dụ chỉ thêm mới dữ liệu, cần xử lý cập nhật nếu có
    for index, row in chi_nhanh_df.iterrows():
        cursor.execute("""
            INSERT INTO chi_nhanh_table (ma_chi_nhanh, ten_chi_nhanh, tinh_thanh_pho, last_updated)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM chi_nhanh_table WHERE ma_chi_nhanh = %s
            )
        """, (row['Mã chi nhánh'], row['Tên chi nhánh'], row['Tỉnh thành phố'], row['last_updated'], row['Mã chi nhánh']))
    for index, row in kpi_df.iterrows():
        cursor.execute("""
            INSERT INTO kpi_table (nam, tinh_thanh_pho, kpi, last_updated)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM kpi_table WHERE nam = %s AND tinh_thanh_pho = %s
            )
        """, (row['Năm'], row['Chi nhánh'], row['KPI'], row['last_updated'], row['Năm'], row['Chi nhánh']))

    for index, row in khach_hang_df.iterrows():
        cursor.execute("""
            INSERT INTO khach_hang_table (ma_kh, khach_hang, last_updated)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM khach_hang_table WHERE ma_kh = %s
            )
        """, (row['Mã KH'], row['Khách hàng'], row['last_updated'], row['Mã KH']))

    for index, row in san_pham_df.iterrows():
        cursor.execute("""
            INSERT INTO san_pham_table (ma_san_pham, san_pham, nhom_san_pham, last_updated)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM san_pham_table WHERE ma_san_pham = %s
            )
        """, (row['Mã Sản phẩm'], row['Sản phẩm'], row['Nhóm sản phẩm'], row['last_updated'], row['Mã Sản phẩm']))

    for index, row in nhan_vien_df.iterrows():
        cursor.execute("""
            INSERT INTO nhan_vien_table (ma_nhan_vien_ban, nhan_vien_ban, last_updated)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM nhan_vien_table WHERE ma_nhan_vien_ban = %s
            )
        """, (row['Mã nhân viên bán'], row['Nhân viên bán'], row['last_updated'], row['Mã nhân viên bán']))

    for index, row in du_lieu_ban_hang_df.iterrows():
        cursor.execute("""
            INSERT INTO du_lieu_ban_hang_table (don_hang, ma_san_pham, ngay_hach_toan, ma_kh, ma_nhan_vien_ban, ma_chi_nhanh, so_luong_ban, don_gia, doanh_thu, gia_von_hang_hoa, last_updated)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM du_lieu_ban_hang_table WHERE don_hang = %s AND ma_san_pham = %s
            )
        """, (row['Đơn hàng'], row['Mã Sản Phẩm'], row['Ngày hạch toán'], row['Mã KH'], row['Mã nhân viên bán'], row['Chi nhánh'], row['Số lượng bán'], row['Đơn giá'], row['Doanh thu'], row['Giá vốn hàng hóa'], row['last_updated'], row['Đơn hàng'], row['Mã Sản Phẩm']))



    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email': ['tienmanh1609jike@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'get_minio_import_starrocks',
    default_args=default_args,
    description='Get MinIO import to PostgreSql Database',
    schedule='0 10 * * *', 
)

with DAG(
    'get_minio_import_starrocks',
    default_args=default_args,
    description='Import Excel files to Oracle Database',
    schedule='0 10 * * *', 
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
) as dag:
    upload_task = PythonOperator(
        task_id='get_data_upload_minio',
        python_callable=upload_file_to_minio,
        dag=dag,
    )

    import_task = PythonOperator(
        task_id='upload_data_minio_to_starrocks',
        python_callable=import_excel_to_starrocks,
        dag=dag,
    )
    transform_and_load = BashOperator(
        task_id='transform_and_load',
        bash_command='cd /opt/airflow/dbt && dbt run --project-dir /opt/airflow/dbt/demo_starrocks',
        dag=dag,
    )

    upload_task >> import_task >> transform_and_load