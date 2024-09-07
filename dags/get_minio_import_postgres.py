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


minio = MinioHelper()
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")


def postgres_connection():
    connection = psycopg2.connect(
        database="db_postgres",
        user='postgres',
        password='postgres',
        host='host.docker.internal',
        port='5434'
)
    return connection

def get_last_run_time(cursor):
    cursor.execute("SELECT MAX(last_updated) FROM KPI_TABLE")
    last_run_time = cursor.fetchone()[0]
    return last_run_time if last_run_time else datetime(1900, 1, 1)

files = [{"name":"KPI_Template.xlsx"},
        {"name":"Modeling_and_DAX.xlsx"}
        ] 
def upload_file_to_minio():
    date = datetime.today().date()
    for item in files:
        with open(f'/opt/airflow/data/{item.get("name")}', 'rb') as file:
            # Ghi tệp lên MinIO
            minio.upload_xlsx_to_minio(file, object_name=f'raw_data/{date}/{item.get("name")}')

def clean_data(df):
    # Remove row null
    df = df.dropna()

    # Remove special characters
    df = df.applymap(lambda x: re.sub(r'[^a-zA-Z0-9\sáàảãạâấầẩẫậăắằẳẵặêếềểễệîìíĩịòôốồổỗộơớờởỡợuúùủũụưứừửữựýỳỷỹỵ]', '', x) if isinstance(x, str) else x)

    return df


def import_excel_to_postgres():
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

    # Clean data
    kpi_df = clean_data(kpi_df)
    khach_hang_df = clean_data(khach_hang_df)
    san_pham_df = clean_data(san_pham_df)
    nhan_vien_df = clean_data(nhan_vien_df)
    du_lieu_ban_hang_df = clean_data(du_lieu_ban_hang_df)
    chi_nhanh_df = clean_data(chi_nhanh_df)


    # Add last_updated column
    khach_hang_df['last_updated'] = pd.to_datetime('now')
    san_pham_df['last_updated'] = pd.to_datetime('now')
    nhan_vien_df['last_updated'] = pd.to_datetime('now')
    du_lieu_ban_hang_df['last_updated'] = pd.to_datetime('now')
    chi_nhanh_df['last_updated'] = pd.to_datetime('now')

    conn = postgres_connection()
    cursor = conn.cursor()

    last_run_time = get_last_run_time(cursor)

    # Filter data based on last_updated
    kpi_df = kpi_df[kpi_df['last_updated'] > last_run_time]
    khach_hang_df = khach_hang_df[khach_hang_df['last_updated'] > last_run_time]
    san_pham_df = san_pham_df[san_pham_df['last_updated'] > last_run_time]
    nhan_vien_df = nhan_vien_df[nhan_vien_df['last_updated'] > last_run_time]
    du_lieu_ban_hang_df = du_lieu_ban_hang_df[du_lieu_ban_hang_df['last_updated'] > last_run_time]
    chi_nhanh_df = chi_nhanh_df[chi_nhanh_df['last_updated'] > last_run_time]

    # Insert or update data to Oracle Database
    for index, row in chi_nhanh_df.iterrows():
        cursor.execute("""
            INSERT INTO chi_nhanh_table (ma_chi_nhanh, ten_chi_nhanh, tinh_thanh_pho, last_updated)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ma_chi_nhanh)
            DO UPDATE SET ten_chi_nhanh = EXCLUDED.ten_chi_nhanh, tinh_thanh_pho = EXCLUDED.tinh_thanh_pho, last_updated = EXCLUDED.last_updated
        """, (row['Mã chi nhánh'], row['Tên chi nhánh'], row['Tỉnh thành phố'], row['last_updated']))
    for index, row in kpi_df.iterrows():
        cursor.execute("""
            INSERT INTO kpi_table (nam, tinh_thanh_pho, kpi, last_updated)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (nam, tinh_thanh_pho) 
            DO UPDATE SET kpi = EXCLUDED.kpi, last_updated = EXCLUDED.last_updated
        """, (row['Năm'], row['Chi nhánh'], row['KPI'], row['last_updated']))
    for index, row in khach_hang_df.iterrows():
        cursor.execute("""
            INSERT INTO khach_hang_table (ma_kh, khach_hang, last_updated)
            VALUES (%s, %s, %s)
            ON CONFLICT (ma_kh)
            DO UPDATE SET khach_hang = EXCLUDED.khach_hang, last_updated = EXCLUDED.last_updated
        """, (row['Mã KH'], row['Khách hàng'], row['last_updated']))

    for index, row in san_pham_df.iterrows():
        cursor.execute("""
            INSERT INTO san_pham_table (ma_san_pham, san_pham, nhom_san_pham, last_updated)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ma_san_pham)
            DO UPDATE SET san_pham = EXCLUDED.san_pham, nhom_san_pham = EXCLUDED.nhom_san_pham, last_updated = EXCLUDED.last_updated
        """, (row['Mã Sản phẩm'], row['Sản phẩm'], row['Nhóm sản phẩm'], row['last_updated']))

    for index, row in nhan_vien_df.iterrows():
        cursor.execute("""
            INSERT INTO nhan_vien_table (ma_nhan_vien_ban, nhan_vien_ban, last_updated)
            VALUES (%s, %s, %s)
            ON CONFLICT (ma_nhan_vien_ban)
            DO UPDATE SET nhan_vien_ban = EXCLUDED.nhan_vien_ban, last_updated = EXCLUDED.last_updated
        """, (row['Mã nhân viên bán'], row['Nhân viên bán'], row['last_updated']))

    for index, row in du_lieu_ban_hang_df.iterrows():
        cursor.execute("""
            INSERT INTO du_lieu_ban_hang_table (ngay_hach_toan, don_hang, ma_kh, ma_san_pham, so_luong_ban, don_gia, doanh_thu, gia_von_hang_hoa, ma_nhan_vien_ban, ma_chi_nhanh, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (don_hang, ma_san_pham)
            DO UPDATE SET ngay_hach_toan = EXCLUDED.ngay_hach_toan, ma_kh = EXCLUDED.ma_kh,
                        so_luong_ban = EXCLUDED.so_luong_ban, don_gia = EXCLUDED.don_gia,
                        doanh_thu = EXCLUDED.doanh_thu, gia_von_hang_hoa = EXCLUDED.gia_von_hang_hoa,
                        ma_nhan_vien_ban = EXCLUDED.ma_nhan_vien_ban, ma_chi_nhanh = EXCLUDED.ma_chi_nhanh,
                        last_updated = EXCLUDED.last_updated
        """, (row['Ngày hạch toán'], row['Đơn hàng'], row['Mã KH'], row['Mã Sản Phẩm'], row['Số lượng bán'],
            row['Đơn giá'], row['Doanh thu'], row['Giá vốn hàng hóa'], row['Mã nhân viên bán'], row['Chi nhánh'], row['last_updated']))




    conn.commit()
    cursor.close()
    conn.close()


def drop_datamart_func():
    conn = oracle_connection()
    cursor = conn.cursor()
    drop_datamart(cursor)
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
    'get_minio_import_postgres',
    default_args=default_args,
    description='Get MinIO import to PostgreSql Database',
    schedule='0 10 * * *', 
)

with DAG(
    'get_minio_import_postgres',
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
        task_id='upload_data_minio_to_postgres',
        python_callable=import_excel_to_postgres,
        dag=dag,
    )

    # drop_datamart_task = PythonOperator(
    #     task_id='drop_datamart',
    #     python_callable=drop_datamart_func,
    # )

    

    transform_and_load = BashOperator(
    task_id='transform_and_load',
    bash_command='cd /opt/airflow/dbt && dbt run --project-dir /opt/airflow/dbt/datamart_postgres',
    dag=dag,
    )

    upload_task >> import_task >> transform_and_load



    
