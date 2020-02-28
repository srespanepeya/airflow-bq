from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable

# Parametros del DAG
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
    
dag_name = 'GSOD_Calcutations' #This must be unique!!! 

with DAG(dag_name, schedule_interval='00 12 * * *', catchup=False, default_args=default_args) as dag:

    # DELETE YESTERDAY DATA
    bq_create_master_table = BigQueryOperator(
        task_id='bq_create_master_table',
        sql='sql/00_create_master_table.sql',
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery'
    )

    # EXTRACT YESTERDAY DATA
    bq_calculate_hot_days = BigQueryOperator(
        task_id='bq_calculate_hot_days',
        sql='sql/bq_calculate_hot_days.sql',
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery'
    )

    # SLACK ERROR OR SUCCESS
    slack_success = SlackAPIPostOperator(
        dag=dag,
        task_id='slack-success',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        channel=Variable.get('slack_channel'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':heavy_check_mark: - {time} - {dag} has completed'.format(
            dag='dag --> {}'.format(dag_name),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
        ),
        retries=0
    )

    slack_error = SlackAPIPostOperator(
        dag=dag,
        task_id='slack-error',
        trigger_rule=TriggerRule.ONE_FAILED,
        channel=Variable.get('slack_channel'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':exclamation: - {time} - {dag} has not completed'.format(
            dag='dag --> {}'.format(dag_name),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
        ),
        retries=0
    )

    # Define the DAG structure. Precedence for tasks

    bq_create_master_table >> bq_calculate_hot_days  >> [slack_success, slack_error]
