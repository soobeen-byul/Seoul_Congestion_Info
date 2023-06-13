from airflow import DAG
from datetime import datetime
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.hooks.hive import *
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
import pandas as pd

HQL_PATH = Variable.get("SEOUL_HQL_PATH")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1)
}

test_dag = DAG(
    'hive_test',
    default_args=default_args,
    schedule_interval="* */10 * * *"
)


# def simple_query():
#     hql = "SELECT * FROM raw_seoul LIMIT 10"
#     hm = HiveServer2Hook(hiveserver2_conn_id = 'Hiveserver2_test')
#     result = hm.get_records(hql)
    
#     print(result)
    
#     for i in result:
#     	print(i)
        
    
# t1 = PythonOperator(
#     task_id = 'HiveServer2Hook_test',
#     python_callable=simple_query,
#     dag=test_dag,
# )

# hql= '''
#      CREATE TABLE bbb AS SELECT * from raw_seoul limit 10
#      '''


# h1 = HiveOperator(
#     task_id='HiveOperator_test',
#     hql=hql,
#     hive_cli_conn_id='hive_cli_connect',
#     run_as_owner=True,
#     dag=test_dag,
# )

def hive_test():

    arr = [[1,2],[3,4]]
    df = pd.DataFrame(arr,columns=['a','b']) 
    
    print(df)
    hh = HiveCliHook(hive_cli_conn_id='hive_cli_connect')
    hh.load_df(df=df,table='test',
               field_dict={
                            'a':'INT',
                            'b':'INT'
                        })
    
i1 = PythonOperator(task_id='Insert.live',
                    python_callable=hive_test,
                    dag=test_dag)