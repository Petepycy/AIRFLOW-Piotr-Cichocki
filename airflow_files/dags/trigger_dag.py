import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.models import DagRun
from smart_file_sensor import SmartFileSensor
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'Peter',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        'trigger_dag', default_args=default_args, schedule=None
) as dag:
    sensor = SmartFileSensor(task_id='sensor_wait_run_file', fs_conn_id='fs_default',
                             filepath=Variable.get('run', default_var='/opt/airflow/files/run'))
    trigger_dag = TriggerDagRunOperator(task_id='Trigger_Dag', trigger_dag_id='table_name_1')

    with TaskGroup(group_id='Process_results_SubDag') as tg1:
        def get_most_recent_dag_run(dt):
            dag_runs = DagRun.find(dag_id="table_name_1")
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            if dag_runs:
                return dag_runs[0].execution_date


        wait_for_dag = ExternalTaskSensor(task_id='ExternalTaskSensor', external_dag_id='table_name_1',
                                          external_task_id=None, execution_date_fn=get_most_recent_dag_run,
                                          mode="reschedule", trigger_rule=TriggerRule.ALL_SUCCESS)


        @dag.task()
        def print_result():
            context = get_current_context()
            message = context['task_instance'].xcom_pull(dag_id='table_name_1', task_ids='query_the_table',
                                                         include_prior_dates=True)
            if not message:
                raise ValueError('No value in xcoms')
            logging.info(message)


        remove_file = BashOperator(task_id='Remove_run_file', bash_command=f'rm {Variable.get("run", default_var="/opt/airflow/files/run")}')
        create_timestamp = BashOperator(task_id='create_finished_timestamp',
                                        bash_command=f'touch /opt/airflow/files/finished_' + '{{ ts_nodash }}')

        wait_for_dag >> print_result() >> remove_file >> create_timestamp

    alert_slack = SlackAPIPostOperator(
        task_id="slack_message",
        slack_conn_id="slack_conn",
        token=Variable.get('slack_token'),
        text=f"trigger_dag finished {datetime.now()}",
        channel="#random",
    )

    sensor >> trigger_dag >> tg1 >> alert_slack
