# let's send slack message using WebHooks API

# airflow stuff
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# request-response stuff
import requests, json

# date stuff
from datetime import datetime, timedelta

# static constants as keys
WEBHOOK_URL_KEY = 'ANZOR_BOT_WEBHOOK_URL'
SLACK_MESSAGE_KEY = 'SLACK_MESSAGE'
SCHEDULE_INTERVAL_KEY = 'SCHEDULE_INTERVAL'
VARIABLE_FILENAME = 'airflow_variables'

# data storage dictionary
AIRFLOW_VARIABLES = Variable.get(VARIABLE_FILENAME, deserialize_json=True)

# values from dict
WEBHOOK_URL = AIRFLOW_VARIABLES[WEBHOOK_URL_KEY]
SLACK_MESSAGE = AIRFLOW_VARIABLES[SLACK_MESSAGE_KEY]  # will be formatted later
SCHEDULE_INTERVAL = AIRFLOW_VARIABLES[SCHEDULE_INTERVAL_KEY]

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

# DAG
dag = DAG(
    'Slack_DAG',
    default_args=default_args,
    catchup=False,
    description='My first slack DAG',
    schedule_interval=timedelta(seconds=SCHEDULE_INTERVAL),
)


# send configurable slack message
def send_slack_message():
    today = str(datetime.today().strftime('%Y-%m-%d-%H:%M:%S'))
    text_message = str(SLACK_MESSAGE).format(today)
    json_payload = json.dumps({'text': text_message})
    response = requests.post(WEBHOOK_URL,
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
