import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Stephen',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['test@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag_start_stop_commands = DAG(
'dag_start_stop_commands',
default_args=default_args,
# schedule_interval='0 0 * * *',
schedule_interval=timedelta(minutes=10),
start_date=datetime(2021, 1, 1),
# dagrun_timeout=timedelta(minutes=6),
description='executing start and stop commands ',
)

start_hadoop = BashOperator(
                task_id="start_task",
                bash_command="start-all.sh ",
          dag=dag_start_stop_commands
            )

start_spark  = BashOperator(
        task_id="start_spark",
        bash_command="pyspark ",
        dag=dag_start_stop_commands,
        run_as_user='stephen'
            )
# print("creating a directory")
cd_kafka_home = BashOperator(
                task_id="cd_kafka_home",
                bash_command="cd /home/stephen/opt/kafka_2.3.1 ",
            do_xcom_push=True,
            dag=dag_start_stop_commands,
        run_as_user='stephen'
            )
start_zookeeper = BashOperator(
                task_id="start_zookeeper",
                bash_command="bin/zookeeper-server-start.sh config/zookeeper.properties ",
            do_xcom_push=True,
            dag=dag_start_stop_commands,
        run_as_user='stephen'
            )

start_kafka_server = BashOperator(
                task_id="start_kafka_server",
                bash_command="bin/kafka-server-start.sh config/server.properties ", 
                do_xcom_push=True, 
            dag=dag_start_stop_commands,
        run_as_user='stephen'
            )




start_hadoop>>start_spark>>cd_kafka_home>>[start_zookeeper, start_kafka_server]


if __name__ == '__main__ ':
  dag_start_stop_commands.cli()