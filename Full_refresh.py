from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import airflow
from airflow.models import DAG,TaskInstance,BaseOperator
from airflow import models, settings
from airflow.utils.trigger_rule import TriggerRule

#seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
#                                      datetime.min.time())

default_args = {
    'owner': 'airflowownername',
    'run_as_user': 'userid',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 5),
    'email': ['shobhit300@gmail.com'],
#    'retries': 2,
#  'retry_delay': timedelta(minutes=1),
}

def sub_dag2(parent_dag_name, child_dag_name2,default_args):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name2),
    default_args=default_args,
      )

  ti=airflow.models.TaskInstance

  def main():
       cob_cmd = "ssh " + "userid" + "@" + "servername" + " " + "/usr/scripts/launch.sh cob cob_log4j.properties COB.jar com.abc.ar1.driver.Cob cob.properties commonFunctions-1.7.jar "
       mbr_cmd = "ssh " + "userid" + "@" + "servername" + " " + "/usr/scripts/launch.sh member member_log4j.properties Member.jar com.abc.ar1.driver.member member.properties commonFunctions-1.7.jar "
       mbr_medicare_cmd = "ssh " + "userid" + "@" + "servername" + " " + "/usr/scripts/launch.sh mmbmdr mbr_mdr_log4j.properties Mbr_Mdr.jar com.abc.ar1.driver.memberMedicare mbr_mdr.properties commonFunctions-1.7.jar "

     t1 = BashOperator(
    	     task_id='Cob',
    	     bash_command=cob_cmd,
    	     on_failure_callback=ticket,
            provide_context=True,
    	     dag=dag)
       t2 = BashOperator(
    	     task_id='Member',
    	     bash_command=mbr_cmd,
    	     on_failure_callback=ticket,
    	     provide_context=True,
    	     dag=dag)
       t3 = BashOperator(
    	     task_id='Member_Mdr',
    	     bash_command=mbr_mdr_cmd,
    	     on_failure_callback=ticket,
    	     provide_context=True,
    	     dag=dag)

       t1.set_downstream(t2)
       t2.set_downstream(t3)

  def ticket(context):
       ti = context['ti']
       task_id = ti.task_id
       dag_id = ti.dag_id
       inc_cmd = "sh /usr/local/airflow/scripts/incident_prd.sh" + " " + task_id + " " +  dag_id
       task_incident = BashOperator(
     			task_id='Raise_Incident',
     			bash_command= inc_cmd,
     			dag=dag)
       return task_incident.execute(context=context)

  main()

  return dag
