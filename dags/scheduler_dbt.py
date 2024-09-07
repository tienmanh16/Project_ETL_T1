# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.docker_operator import DockerOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import oracledb
# from plugins.config import config
# from datetime import timedelta
# from datetime import datetime
# import pendulum
# import cx_Oracle
# from sqlalchemy import types, create_engine

# local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# def oracle_connection():
#     dsn = oracledb.makedsn(config.DB_HOST, config.DB_PORT, service_name=config.DB_SERVICE)
#     connection = oracledb.connect(user=config.DB_USER, password=config.DB_PASSWORD, dsn=dsn)
#     return connection

# def import_data_to_oracle():
#     kpi_file = '/opt/airflow/data/KPI_Template.xlsx'
#     modeling_dax_file = '/opt/airflow/data/Modeling_and_DAX.xlsx'

#     # Read file KPI Template
#     kpi_df = pd.read_excel(kpi_file, sheet_name='KPI theo năm')
#     kpi_df['KPI'] = kpi_df['KPI'].str.replace(',', '', regex=False).astype(int)
#     # kpi_df['last_updated'] = datetime.now() 
#     # print(kpi_df.head())
#     # print(kpi_df.dtypes)
#     # Reads sheets in Modeling and DAX file
#     khach_hang_df = pd.read_excel(modeling_dax_file, sheet_name='Khách hàng')
#     san_pham_df = pd.read_excel(modeling_dax_file, sheet_name='Sản phẩm')
#     nhan_vien_df = pd.read_excel(modeling_dax_file, sheet_name='Nhân viên')
#     du_lieu_ban_hang_df = pd.read_excel(modeling_dax_file, sheet_name='Dữ liệu bán hàng')
#     chi_nhanh_df = pd.read_excel(modeling_dax_file, sheet_name='Chi nhánh')
#     conn = oracle_connection()
#     cursor = conn.cursor()

#     for index, row in kpi_df.iterrows():
#         cursor.execute("INSERT INTO KPI_TABLE (Nam, Chi_nhanh, KPI) VALUES (:1, :2, :3)",
#                        (row['Năm'], row['Chi nhánh'], row['KPI']))

#     for index, row in khach_hang_df.iterrows():
#         cursor.execute("INSERT INTO KHACH_HANG_TABLE (Ma_KH, Khach_hang) VALUES (:1, :2)",
#                        (row['Mã KH'], row['Khách hàng']))

#     for index, row in san_pham_df.iterrows():
#         cursor.execute("INSERT INTO SAN_PHAM_TABLE (Ma_San_Pham, San_pham, Nhom_san_pham) VALUES (:1, :2, :3)",
#                        (row['Mã Sản phẩm'], row['Sản phẩm'], row['Nhóm sản phẩm']))

#     for index, row in nhan_vien_df.iterrows():
#         cursor.execute("INSERT INTO NHAN_VIEN_TABLE (Ma_nhan_vien_ban, Nhan_vien_ban) VALUES (:1, :2)",
#                        (row['Mã nhân viên bán'], row['Nhân viên bán']))

#     for index, row in du_lieu_ban_hang_df.iterrows():
#         cursor.execute(
#             "INSERT INTO DU_LIEU_BAN_HANG_TABLE (Ngay_hach_toan, Don_hang, Ma_KH, Ma_san_pham, So_luong_ban, Don_gia, Doanh_thu, Gia_von_hang_hoa, Ma_nhan_vien_ban, Chi_nhanh) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)",
#             (row['Ngày hạch toán'], row['Đơn hàng'], row['Mã KH'], row['Mã Sản Phẩm'], row['Số lượng bán'],
#              row['Đơn giá'], row['Doanh thu'], row['Giá vốn hàng hóa'], row['Mã nhân viên bán'], row['Chi nhánh']))

#     for index, row in chi_nhanh_df.iterrows():
#         cursor.execute("INSERT INTO CHI_NHANH_TABLE (Ma_chi_nhanh, Ten_chi_nhanh, Tinh_thanh_pho) VALUES (:1, :2, :3)",
#                        (row['Mã chi nhánh'], row['Tên chi nhánh'], row['Tỉnh thành phố']))
#     conn.commit()
#     cursor.close()
#     conn.close()


# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 8, 1),
#     'email': ['tienmanh1609jike@gmail.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }   


# dag = DAG(
#     'import_excel_to_oracle',
#     default_args=default_args,
#     description='Import Excel files to Oracle Database and Use DBT transform to SQL Server',
#     schedule='0 10 * * *', 
# )

# load_to_oracle = PythonOperator(
#     task_id='load_to_oracle',
#     python_callable=import_data_to_oracle,
#     dag=dag,
# )

# run_dbt = DockerOperator(
#     task_id='run_dbt',
#     image='your_dbt_image',  # Tên image Docker của dbt
#     api_version='auto',
#     auto_remove=True,
#     command='dbt run --profiles-dir /path/to/dbt/profiles',  # Điều chỉnh đường dẫn
#     docker_url='unix://var/run/docker.sock',
#     network_mode='bridge',
#     dag=dag,
# )

# # Task 2: Transform with dbt and load to SQL Server
# # transform_and_load = BashOperator(
# #     task_id='transform_and_load',
# #     bash_command='docker exec -it dbt_container dbt run --profiles-dir .',  # dbt run trong container dbt
# #     dag=dag,
# # )

# load_to_oracle >> run_dbt