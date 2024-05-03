#import block
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

#default arguments block
default_args = {
    "owner": "Sam Nelom",
    "start_date": days_ago(0),
    "email": ["samnelom@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

#dag definition block
dag = DAG(
    dag_id="ETL_toll_data",
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description="Apache Airflow Final Assignment"
)


#task definition block
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="tar -xvf $AIRFLOW_HOME/dags/finalassignment/tolldata.tgz -C $AIRFLOW_HOME/dags/finalassignment",
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command="cut -d ',' -f1-4 $AIRFLOW_HOME/dags/finalassignment/vehicle-data.csv > $AIRFLOW_HOME/dags/finalassignment/csv_data.csv",
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command="cut -d $'\t' -f5-7 $AIRFLOW_HOME/dags/finalassignment/tollplaza-data.tsv | tr '\t' ',' > $AIRFLOW_HOME/dags/finalassignment/tsv_data.csv",
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command="cut -c59-62,63-68 $AIRFLOW_HOME/dags/finalassignment/payment-data.txt | tr ' ' ',' > $AIRFLOW_HOME/dags/finalassignment/fixed_width_data.csv",
    dag=dag
)


consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command="paste -d ',' $AIRFLOW_HOME/dags/finalassignment/csv_data.csv $AIRFLOW_HOME/dags/finalassignment/tsv_data.csv $AIRFLOW_HOME/dags/finalassignment/fixed_width_data.csv > $AIRFLOW_HOME/dags/finalassignment/extracted_data.csv",
    dag=dag
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command="tr '[a-z]' '[A-Z]' < $AIRFLOW_HOME/dags/finalassignment/extracted_data.csv > $AIRFLOW_HOME/dags/finalassignment/staging/transformed_data.csv",
    dag=dag
)

#task pipeline block
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data