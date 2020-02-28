from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable
import time

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

dag_name = 'BQ_Analytics_GIT_Sync' #This must be unique!!! 

###############################################################################################
#DAG Creation
with DAG(dag_name, schedule_interval=None, catchup=False, default_args=default_args) as dag:
    
    update_airflow_git_repo = BashOperator(
        task_id='update_airflow_git_repo',
        bash_command="""
            cd /home/airflow/airflow/dags/airflow-bq
            pwd
            git stash
            git pull origin master
        """
    )
    # SLACK ERROR OR SUCCESS
    slack_success = SlackAPIPostOperator(
        dag=dag,
        task_id='slack-success',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        channel=Variable.get('slack_channel'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':heavy_check_mark::github:  - {time} - {dag} has synchronized *bi-airflow* git repository in Airflow'.format(
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

    # Set task downstream
    update_airflow_git_repo >> [slack_error,slack_success]

