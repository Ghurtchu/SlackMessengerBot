# let's send slack message using WebHooks API

# import airflow stuff
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# import http stuff
import requests, json

# import date stuff
from datetime import datetime, timedelta

# define static constants
WEBHOOK_URL_KEY = 'ANZOR_BOT_WEBHOOK_URL'
SLACK_MESSAGE_KEY = 'SLACK_MESSAGE'
SCHEDULE_INTERVAL_KEY = 'SCHEDULE_INTERVAL'
VARIABLE_FILENAME = 'airflow_variables'

# retrieve Airflow Variables and store them in a python dict (from json)
airflow_variables = Variable.get(VARIABLE_FILENAME, deserialize_json=True)

# retrieve an each airflow variable by its associated key from 'airflow_variables' dict
webhook_url = airflow_variables[WEBHOOK_URL_KEY]
slack_message = airflow_variables[SLACK_MESSAGE_KEY]  # message will be formatted later
schedule_interval = airflow_variables[SCHEDULE_INTERVAL_KEY]

# today's date
now = datetime.now()

# default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(now.year, now.month, now.day),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# dag
dag = DAG(
    'Slack_DAG',
    default_args=default_args,
    catchup=False,
    description='My first slack DAG',
    schedule_interval=timedelta(seconds=schedule_interval),
)


# send configurable slack message
def send_slack_message():
    today = str(datetime.today().strftime('%Y-%m-%d-%H:%M:%S'))
    text_message = str(slack_message).format(today)
    json_payload = json.dumps({'text': text_message})
    response = requests.post(webhook_url,
                             data=json_payload)
    print(response.text)  # should print OK if status.code == 200 => means everything is ok


# create task
task = PythonOperator(
    task_id='send_slack_message',
    python_callable=send_slack_message,
    dag=dag,
)

# run it
task
