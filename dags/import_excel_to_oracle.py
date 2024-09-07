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

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Connect to Oracle Database
def oracle_connection():
    dsn = oracledb.makedsn(config.DB_HOST, config.DB_PORT, service_name=config.DB_SERVICE)
    connection = oracledb.connect(user=config.DB_USER, password=config.DB_PASSWORD, dsn=dsn)
    return connection

def get_last_run_time(cursor):
    cursor.execute("SELECT MAX(last_updated) FROM KPI_TABLE")
    last_run_time = cursor.fetchone()[0]
    return last_run_time if last_run_time else datetime(1900, 1, 1)

def drop_datamart(cursor):
    try:
        cursor.execute("DROP TABLE BAI_TEST.REVENUE_BY_MONTH_SALEMAN")
        print("Table BAI_TEST.REVENUE_BY_MONTH_SALEMAN dropped successfully.")
    except oracledb.Error as e:
        print(f"Error dropping table REVENUE_BY_MONTH_SALEMAN: {e}")

    try:
        cursor.execute("DROP TABLE BAI_TEST.REVENUE_BY_MONTH_STORE")
        print("Table BAI_TEST.REVENUE_BY_MONTH_STORE dropped successfully.")
    except oracledb.Error as e:
        print(f"Error dropping table REVENUE_BY_MONTH_STORE: {e}")

    try:
        cursor.execute("DROP TABLE BAI_TEST.TEST")
        print("Table BAI_TEST.TEST dropped successfully.")
    except oracledb.Error as e:
        print(f"Error dropping table TEST: {e}")

    try:
        cursor.execute("DROP TABLE BAI_TEST.REVENUE_BY_BRANCH")
        print("Table BAI_TEST.REVENUE_BY_BRANCH dropped successfully.")
    except oracledb.Error as e:
        print(f"Error dropping table REVENUE_BY_BRANCH: {e}")

    try:
        cursor.execute("DROP TABLE BAI_TEST.REVENUE_BY_CUSTOMERR")
        print("Table BAI_TEST.REVENUE_BY_CUSTOMER dropped successfully.")
    except oracledb.Error as e:
        print(f"Error dropping table REVENUE_BY_CUSTOMER: {e}")
        

def clean_data(df):
    # Remove row null
    df = df.dropna()

    # Remove special characters
    df = df.applymap(lambda x: re.sub(r'[^a-zA-Z0-9\sáàảãạâấầẩẫậăắằẳẵặêếềểễệîìíĩịòôốồổỗộơớờởỡợuúùủũụưứừửữựýỳỷỹỵ]', '', x) if isinstance(x, str) else x)

    return df


def import_excel_to_oracle():

    kpi_file = '/opt/airflow/data/KPI_Template.xlsx'
    modeling_dax_file = '/opt/airflow/data/Modeling_and_DAX.xlsx'

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

    conn = oracle_connection()
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
    for index, row in kpi_df.iterrows():
        cursor.execute("""
            MERGE INTO KPI_TABLE k
            USING (SELECT :1 AS Nam, :2 AS Chi_nhanh, :3 AS KPI, :4 AS last_updated FROM dual) src
            ON (k.Nam = src.Nam AND k.Chi_nhanh = src.Chi_nhanh)
            WHEN MATCHED THEN
                UPDATE SET k.KPI = src.KPI, k.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
                INSERT (Nam, Chi_nhanh, KPI, last_updated)
                VALUES (src.Nam, src.Chi_nhanh, src.KPI, src.last_updated)
        """, (row['Năm'], row['Chi nhánh'], row['KPI'], row['last_updated']))
    for index, row in khach_hang_df.iterrows():
        cursor.execute("""
            MERGE INTO KHACH_HANG_TABLE kh
            USING (SELECT :1 AS Ma_KH, :2 AS Khach_hang, :3 AS last_updated FROM dual) src
            ON (kh.Ma_KH = src.Ma_KH)
            WHEN MATCHED THEN
                UPDATE SET kh.Khach_hang = src.Khach_hang, kh.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
                INSERT (Ma_KH, Khach_hang, last_updated)
                VALUES (src.Ma_KH, src.Khach_hang, src.last_updated)
        """, (row['Mã KH'], row['Khách hàng'], row['last_updated']))

    for index, row in san_pham_df.iterrows():
        cursor.execute("""
            MERGE INTO SAN_PHAM_TABLE sp
            USING (SELECT :1 AS Ma_San_Pham, :2 AS San_pham, :3 AS Nhom_san_pham, :4 AS last_updated FROM dual) src
            ON (sp.Ma_San_Pham = src.Ma_San_Pham)
            WHEN MATCHED THEN
                UPDATE SET sp.San_pham = src.San_pham, sp.Nhom_san_pham = src.Nhom_san_pham, sp.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
                INSERT (Ma_San_Pham, San_pham, Nhom_san_pham, last_updated)
                VALUES (src.Ma_San_Pham, src.San_pham, src.Nhom_san_pham, src.last_updated)
        """, (row['Mã Sản phẩm'], row['Sản phẩm'], row['Nhóm sản phẩm'], row['last_updated']))

    for index, row in nhan_vien_df.iterrows():
        cursor.execute("""
            MERGE INTO NHAN_VIEN_TABLE nv
            USING (SELECT :1 AS Ma_nhan_vien_ban, :2 AS Nhan_vien_ban, :3 AS last_updated FROM dual) src
            ON (nv.Ma_nhan_vien_ban = src.Ma_nhan_vien_ban)
            WHEN MATCHED THEN
                UPDATE SET nv.Nhan_vien_ban = src.Nhan_vien_ban, nv.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
                INSERT (Ma_nhan_vien_ban, Nhan_vien_ban, last_updated)
                VALUES (src.Ma_nhan_vien_ban, src.Nhan_vien_ban, src.last_updated)
        """, (row['Mã nhân viên bán'], row['Nhân viên bán'], row['last_updated']))
    
    for index, row in du_lieu_ban_hang_df.iterrows():
        cursor.execute("""
            MERGE INTO DU_LIEU_BAN_HANG_TABLE dlbh
            USING (SELECT :1 AS Ngay_hach_toan, :2 AS Don_hang, :3 AS Ma_KH, :4 AS Ma_san_pham, 
                          :5 AS So_luong_ban, :6 AS Don_gia, :7 AS Doanh_thu, :8 AS Gia_von_hang_hoa, 
                          :9 AS Ma_nhan_vien_ban, :10 AS Chi_nhanh, :11 AS last_updated FROM dual) src
            ON (dlbh.Don_hang = src.Don_hang AND dlbh.Ma_san_pham = src.Ma_san_pham)
            WHEN MATCHED THEN
                UPDATE SET dlbh.Ngay_hach_toan = src.Ngay_hach_toan, dlbh.Ma_KH = src.Ma_KH, 
                           dlbh.So_luong_ban = src.So_luong_ban, dlbh.Don_gia = src.Don_gia, 
                           dlbh.Doanh_thu = src.Doanh_thu, dlbh.Gia_von_hang_hoa = src.Gia_von_hang_hoa, 
                           dlbh.Ma_nhan_vien_ban = src.Ma_nhan_vien_ban, dlbh.Chi_nhanh = src.Chi_nhanh, 
                           dlbh.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
                INSERT (Ngay_hach_toan, Don_hang, Ma_KH, Ma_san_pham, So_luong_ban, Don_gia, Doanh_thu, 
                        Gia_von_hang_hoa, Ma_nhan_vien_ban, Chi_nhanh, last_updated)
                VALUES (src.Ngay_hach_toan, src.Don_hang, src.Ma_KH, src.Ma_san_pham, src.So_luong_ban, 
                        src.Don_gia, src.Doanh_thu, src.Gia_von_hang_hoa, src.Ma_nhan_vien_ban, 
                        src.Chi_nhanh, src.last_updated)
        """, (row['Ngày hạch toán'], row['Đơn hàng'], row['Mã KH'], row['Mã Sản Phẩm'], row['Số lượng bán'],
              row['Đơn giá'], row['Doanh thu'], row['Giá vốn hàng hóa'], row['Mã nhân viên bán'], row['Chi nhánh'], row['last_updated']))

    for index, row in chi_nhanh_df.iterrows():
        cursor.execute("""
            MERGE INTO CHI_NHANH_TABLE cn
            USING (SELECT :1 AS Ma_chi_nhanh, :2 AS Ten_chi_nhanh, :3 AS Tinh_thanh_pho, :4 AS last_updated FROM dual) src
            ON (cn.Ma_chi_nhanh = src.Ma_chi_nhanh)
            WHEN MATCHED THEN
                UPDATE SET cn.Ten_chi_nhanh = src.Ten_chi_nhanh, cn.Tinh_thanh_pho = src.Tinh_thanh_pho, 
                           cn.last_updated = src.last_updated
            WHEN NOT MATCHED THEN
                INSERT (Ma_chi_nhanh, Ten_chi_nhanh, Tinh_thanh_pho, last_updated)
                VALUES (src.Ma_chi_nhanh, src.Ten_chi_nhanh, src.Tinh_thanh_pho, src.last_updated)
        """, (row['Mã chi nhánh'], row['Tên chi nhánh'], row['Tỉnh thành phố'], row['last_updated']))


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
    'import_excel_to_oracle',
    default_args=default_args,
    description='Import Excel files to Oracle Database',
    schedule='0 10 * * *', 
)

with DAG(
    'import_excel_to_oracle',
    default_args=default_args,
    description='Import Excel files to Oracle Database',
    schedule='0 10 * * *', 
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
) as dag:


    import_task = PythonOperator(
        task_id='import_excel',
        python_callable=import_excel_to_oracle,
        dag=dag,
    )

    drop_datamart_task = PythonOperator(
        task_id='drop_datamart',
        python_callable=drop_datamart_func,
    )

    

    transform_and_load = BashOperator(
    task_id='transform_and_load',
    bash_command='cd /opt/airflow/dbt && dbt run --project-dir /opt/airflow/dbt/datamart',
    dag=dag,
    )

    import_task >> drop_datamart_task >> transform_and_load



    
