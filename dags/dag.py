from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from sqlalchemy.types import Integer, Numeric, String
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import os
import sys
print(os.path.abspath("includes.............................."))
sys.path.append(os.path.abspath("includes"))

class Warehouse():
    
    def __init__(self):
        pass
        
    def DBConnect(self, dbName=None):
        """
        A function to connect to SQL database
        """
        mydb = cursor.connect(host='mysql', user='root',
                          password='root', ssl_disabled=True,
                             database=dbName, buffered=True)
        cursor = mydb.cursor()
        return mydb, cursor
    def createDB(self, dbName: str) -> None:
        """
        A function to create SQL database
        """
        mydb, cursor = self.DBConnect()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbName};")
        mydb.commit()
        cursor.close()

    def createTables(self, dbName: str, path: str) -> None:
        """
        A function to create SQL table
        """
        mydb, cursor = self.DBConnect(dbName)
        sqlFile = path
        fd = open(sqlFile, 'r')
        readsqlFile = fd.read()
        fd.close()
        sqlCommands = readsqlFile.split(';')
        for command in sqlCommands:
            try:
                result = cursor.execute(command)
            except Exception as e:
                print('command skipped: ', command)
                print(e)
        mydb.commit()
        cursor.close()
    def insert_into_warehouse(self, dbName: str, df: pd.DataFrame, table_name: str) -> None:
        """
        A function to insert values in SQL table
        """
        mydb, cursor = self.DBConnect(dbName)
        for _, row in df.iterrows():
            sqlQuery = f"""INSERT INTO {table_name} 
            (track_id, cars, traveled_d, avg_speed, lat, lon,
                        speed, lon_acc, lat_acc, time)
                  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            data = (row[0], row[1], row[2], row[3], (row[4]), (row[5]), row[6], row[7], row[8], row[9])
            try:
                cursor.execute(sqlQuery, data)
                mydb.commit()
                print('Data inserted successfully')
            except Exception as e:
                mydb.rollback()
                print('Error: ', e)
if __name__=="__main__":
    wr = Warehouse()
    wr.createDB(dbName='DWH')
    df = pd.read_csv("/opt/airflow/data/data.csv")
    df.drop(["Unnamed: 0"], axis=1, inplace=True)
    wr.createTables(dbName='DWH')
    wr.insert_into_warehouse(dbName = 'DWH', df = df, table_name='elt')

def run_loader(**context):
    data = pd.read_csv("/opt/airflow//data/data.csv")
    print(f"The dataset has {data.columns} columns")
    try:
        conn, cur = wr.DBConnect()
        print("Connected successfully!")
    except Exception as e:
        print(f"error...: {e}")
    
def create_db(**context):
    try:
        new_db = wr.createDB("warehouse")
        print("succesfully created new db {}".format(new_db))
    except Exception as e:
        print(f'error...: {e}')

def create_table(**context):
    try:
        wr.createTables("warehouse", "/includes/schema.sql")
        print("successfully created tables!")
    except Exception as e:
        print(f'error...: {e}')

def split_into_chunks(arr, n):
    return [arr[i: i + n] for i in range(0, len(arr), n)]

def load_data_to_table(**context):
    pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = pg_hook.get_sqlalchemy_engine()
    df = pd.read_csv('/opt/airflow/data/data.csv', skiprows=1, header=None, delimiter="\n")
    series = df[0].str.split(";")
    pd_lines = []
    
    for line in series:
        old_line = [item.strip() for item in line]
        info_index = 4
        info = old_line[:info_index]
        remaining = old_line[info_index:-1]
        chunks = split_into_chunks(remaining, 6)
        for chunk in chunks:
            record = info + chunk
            pd_lines.append(record)
            
    new_df = pd.DataFrame(
        pd_lines,
        columns=[
            "track_id",
            "type",
            "traveled_d",
            "avg_speed",
            "lat",
            "lon",
            "speed",
            "lon_acc",
            "lat_acc",
            "time",
        ],
    )
    new_df.to_sql(
        "open_traffic",
        con=conn,
        if_exists="replace",
        index=True,
        index_label="id",
        dtype={
            "track_id": Integer(),
            "traveled_d": Numeric(),
            "avg_speed": Numeric(),
            "lat": Numeric(),
            "lon": Numeric(),
            "speed": Numeric(),
            "lon_acc": Numeric(),
            "lat_acc": Numeric(),
            "time": Numeric(),
        },
    )

    # try:
    #     df = pd.read_csv("/opt/airflow/data/data.csv")
    #     df.drop(["Unnamed: 0"], axis=1, inplace=True)
    #     wr.insert_into_warehouse(dbName = 'mydatabase', df = df, table_name='elt')
    # except Exception as e:
    #     print(f'error...: {e}')

default_args = {
    'owner':'matilda',
    'retries': 4,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id="workflow",
    default_args=default_args,
    start_date=datetime(2022,7,28),
    schedule_interval='@daily', 
    catchup=False
) as dag:

    #connecting = PythonOperator(
        #task_id = "create_connection",
        #python_callable = run_loader,
        #provide_context=True
        #)

    #new_db = PythonOperator(
        #task_id = "create_new_db",
        #python_callable = create_db,
        #rovide_context=True
    #)

    new_table = PostgresOperator(
        task_id = "create_new_table",
        postgres_conn_id = "postgres_localhost",
        sql = """
        
        create table if not exists open_traffic (
                id serial,
                track_id integer,
                type text,
                traveled_d numeric,
                avg_speed numeric,
                lat numeric,
                lon numeric,
                speed numeric,
                lon_acc numeric,
                lat_acc numeric,
                time numeric
            )"""
    )

    fill_table = PythonOperator(
        task_id = "fill_table",
        python_callable = load_data_to_table,
        
    )

new_table >> fill_table

