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

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Connect to Oracle Database
def oracle_connection():
    dsn = oracledb.makedsn(config.DB_HOST, config.DB_PORT, service_name=config.DB_SERVICE)
    connection = oracledb.connect(user=config.DB_USER, password=config.DB_PASSWORD, dsn=dsn)
    return connection

# def get_last_run_time(cursor):
#     cursor.execute("SELECT MAX(last_updated) FROM KPI_TABLE")
#     last_run_time = cursor.fetchone()[0]
#     return last_run_time if last_run_time else datetime(1900, 1, 1)


def import_csv_to_oracle():
    # Đường dẫn tới tệp CSV
    csv_file_path = '/opt/airflow/data/user.csv'

    # Đọc dữ liệu từ CSV vào DataFrame
    df = pd.read_csv(csv_file_path)

    # Kết nối tới Oracle
    conn = oracle_connection()
    cursor = conn.cursor()

    # Tạo câu lệnh SQL để chèn dữ liệu vào bảng Oracle
    sql_insert = """
    INSERT INTO USER_TABLE (Index, User_Id, First_Name, Last_Name, Sex, Email, Phone, Date_of_birth, Job_Title)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    """

    # Chuyển đổi DataFrame sang dạng list of tuples để chèn vào Oracle
    data = [tuple(x) for x in df.to_numpy()]

    # Thực hiện chèn dữ liệu vào Oracle
    cursor.executemany(sql_insert, data)

    # Commit transaction và đóng kết nối
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
    'import_csv_to_oracle',
    default_args=default_args,
    description='Import Excel files to Oracle Database',
    schedule='0 10 * * *', 
)

with DAG(
    'import_csv_to_oracle',
    default_args=default_args,
    description='Import Excel files to Oracle Database',
    schedule='0 10 * * *', 
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
) as dag:


    import_task = PythonOperator(
        task_id='import_csv',
        python_callable=import_csv_to_oracle,
        dag=dag,
    )

    import_task 

#test
def test_oracle_connection():
    try:
        conn = oracle_connection()
        print("Kết nối đến Oracle Database thành công!")
        conn.close()
    except Exception as e:
        print(f"Không thể kết nối đến Oracle Database: {e}")


test_oracle_connection()

    
