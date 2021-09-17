import csv
from io import BytesIO, StringIO

from airflow.models import DAG, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago

from airflow.operators.email_operator import EmailOperator
from tempfile import NamedTemporaryFile

from datetime import datetime
from datetime import timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Calcutta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 14, tzinfo=local_tz),
    'email': ['vinod@unityhealth.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
 #   'schedule_interval': '@hourly',
}



def download_file(task_instance: TaskInstance, **kwargs):
    sftp_hook = SSHHook(ssh_conn_id="sftp_default")
    sftp_client = sftp_hook.get_conn().open_sftp()
    fp = BytesIO()
    input_path = kwargs["templates_dict"]["input_path"]
    sftp_client.getfo(input_path, fp)
    task_instance.xcom_push(key="raw_input_file", value=fp.getvalue().decode("utf-8"))


def process_file(task_instance: TaskInstance, **kwargs):
    raw_input_file = task_instance.xcom_pull(task_ids="download_file", key="raw_input_file")
    output_rows = []
    for row in csv.reader(StringIO(raw_input_file)):
        row.append("processed")
        output_rows.append(row)
    fp = StringIO()
    writer = csv.writer(fp)
    writer.writerows(output_rows)
    task_instance.xcom_push(key="raw_processed_file", value=fp.getvalue())


def upload_file(task_instance, **kwargs):
    raw_processed_file = task_instance.xcom_pull(task_ids="process_file", key="raw_processed_file")
    sftp_hook = SSHHook(ssh_conn_id="sftp_default")
    sftp_client = sftp_hook.get_conn().open_sftp()
    output_path = kwargs["templates_dict"]["output_path"]
    sftp_client.putfo(BytesIO(raw_processed_file.encode("utf-8")), output_path)


with DAG("sftp_hooks_workflow",
      #   schedule_interval='*/10 * * * *',
         default_args=default_args,
         #start_date=days_ago(2)
         ) as dag:

    sensor = SFTPSensor(task_id="check-for-file",
                        sftp_conn_id="sftp_default",
                        path="/inbound/input.csv",
                        poke_interval=10)

    download_file = PythonOperator(task_id="download_file",
                                   python_callable=download_file,
                                   templates_dict={
                                       "input_path": "/inbound/input.csv",
                                   })

    process_file = PythonOperator(task_id="process_file",
                                  python_callable=process_file)

    upload_file = PythonOperator(task_id="upload_file",
                                 python_callable=upload_file,
                                 templates_dict={
                                     "output_path": "/outbound/{{ ts_nodash }}_output.csv"
                                 })
    email_op = EmailOperator(
            task_id='send_email',
            to="vinod@unityhealth.net",
            subject="Test Email From Unity Airflow ::: Please Ignore",
            html_content=None,
            
        )
        
        
    sensor >> download_file >> process_file >> upload_file >> email_op
