from airflow import DAG
from datetime import datetime, timedelta
from airflow import AirflowException
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'bq_analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 22),
    'email': ['matias.menendez@pedidosya.com','federico.rubinstein@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 6,
    'retry_delay': timedelta(minutes=10)
}


from_interval = -1
to_interval = -1

today_date_check = datetime.strftime(datetime.now(), "%Y-%m-%d")
order_status_dataset = 'Order_Status'
order_status_table = 'Order_Status_Detailed'

shop_list_dataset = 'Shop_List'
shop_list_table = 'orders_last_shop_delivery_time'

destination_dataset = 'user_matias_menendez'
tes_table = 'tes_test'
rnao_date = 'extract_rejected_never_arrived_order_date'


def f_check_data(pDataset,pTable,**kwargs):
    query = """ 
                SELECT CAST(DATE(MAX(update_date)) AS STRING) as updated_date
                FROM Audit.Uploaded_Data UD 
                WHERE  UD.dataset='{dataset}' 
                  AND  UD.table='{table}'
            """.format(dataset=pDataset,table=pTable)
    df = BigQueryHook('peya_bigquery').get_pandas_df(query)
    updated_date_value = df['updated_date'].values[0]
    global today_date_check
    if updated_date_value != today_date_check:
        raise ValueError('The table is not uploaded')
    return 0


dag_name = 'Logistics_Autimation_Example' #This must be unique!!! 

with DAG(dag_name, schedule_interval='00 13 * * *', catchup=False, default_args=default_args) as dag:

    check_order_status = PythonOperator(
        task_id='check_order_status',
        provide_context=True,
        python_callable=f_check_data,
        op_kwargs={'pDataset':order_status_dataset,
                   'pTable':order_status_table}
    )

    check_shop_list = PythonOperator(
        task_id='check_shop_list',
        provide_context=True,
        python_callable=f_check_data,
        op_kwargs={'pDataset':shop_list_dataset,
                   'pTable':shop_list_table}
    )

    dependencies_met = DummyOperator(task_id='dependencies_met')

    bq_delete_tes_test_yesterday_data = BigQueryOperator(
        task_id='bq_delete_tes_test_yesterday_data',
        sql='sql/0000_delete_yesterday_data.sql',
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery',
        params={'from_interval': from_interval,
                'to_interval': to_interval,
                'dataset' : destination_dataset,
                'table' : tes_table,
                'date_field' : 'date'
                }
    )

    # EXTRACT YESTERDAY DATA
    bq_extract_tes_test_yesterday_data = BigQueryOperator(
        task_id='bq_extract_tes_test_raw_data',
        sql='sql/0001_extract_tes_test_data.sql',
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery',
        params={'from_interval': from_interval,
                'to_interval': to_interval
                }
    )

    bq_delete_rnao_date_yesterday_data = BigQueryOperator(
        task_id='bq_delete_rnao_date_yesterday_data',
        sql='sql/0000_delete_yesterday_data.sql',
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery',
        params={'from_interval': from_interval,
                'to_interval': to_interval,
                'dataset' : destination_dataset,
                'table' : rnao_date,
                'date_field' : 'created_date'
                }
    )

    bq_extract_rnao_date_yesterday_data = BigQueryOperator(
        task_id='bq_extract_rnao_date_yesterday_data',
        sql='sql/0002_extract_rejected_never_arrived_order_date.sql',
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery',
        params={'from_interval': from_interval,
                'to_interval': to_interval
                }
    )


[check_order_status,check_shop_list] >> dependencies_met 
dependencies_met >> bq_delete_tes_test_yesterday_data >> bq_extract_tes_test_yesterday_data
dependencies_met >> bq_delete_rnao_date_yesterday_data >> bq_extract_rnao_date_yesterday_data
