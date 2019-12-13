import airflow
import os
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.subdag_operator import SubDagOperator
from bd_preprocessing import sub_dag
from full_refresh import sub_dag2
from datetime import date
from datetime import time
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.email import send_email
from airflow.operators.email_operator import EmailOperator


default_args = {
    'owner': ‘Shobhit’,
    'run_as_user': ‘shobh1’,
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 5),
    'email': ['shobhit300@gmail.com']
}

parent_dag_name = ‘trigger_monthly_process’
child_dag_name1 = 'preprocessing'
child_dag_name2 = 'full_refresh'



main_dag = DAG(dag_id=parent_dag_name, catchup=False,  default_args=default_args, schedule_interval=None)

task1 = BashOperator(
    task_id='Process_Start',
    bash_command='echo "MONTHLY PROCESS STARTS NOW.."',
    dag=main_dag)


subdag1 = BashOperator(
   task_id='Preprocessing',
   bash_command="ssh " + “owner_name” + "@" + "servername" + " " + “/path/monthly_preprocessing.sh /mapr/data/commonFunctions-1.7.jar",
   default_args=default_args,
   dag=main_dag)

sub_dag2 = SubDagOperator(
   subdag=sub_dag2(parent_dag_name, child_dag_name2,default_args),
   task_id=child_dag_name2,
   default_args=default_args,
   dag=main_dag)

today = date.today()
today = str(today)
task_completion_email = EmailOperator(
     			 task_id='Load_completion_email_notify',
                      to= ['shobhit300l@gmail.com'],
                      subject='Monthly Load Complete',
                      html_content="Monthly Load is completed today : " + "" + today,
                      dag=main_dag)

task1.set_downstream(subdag1)
subdag1.set_downstream(sub_dag2)
sub_dag2.set_downstream(task_completion_email)
