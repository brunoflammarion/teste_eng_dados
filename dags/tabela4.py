
# imports
from airflow import DAG
from datetime import datetime, timedelta
# we need to import the bigquery operator - there are lots of cool operators for different tasks and systems, you can also build your own
from airflow.contrib.operators.bigquery_operator import BigQueryOperator 

# imports
from airflow import DAG
from datetime import datetime, timedelta
# we need to import the bigquery operator - there are lots of cool operators for different tasks and systems, you can also build your own
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# create a dictionary of default typical args to pass to the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # does this dag depend on the previous run of the dag? best practice is to try have dags not depend on state or results of a previous run
    'start_date': datetime(2020, 10, 30), # from what date to you want to pretend this dag was born on? by default airflow will try backfill - be careful
    'retries': 1, # if fails how many times should we retry?
    'retry_delay': timedelta(minutes=2), # if we need to retry how long should we wait before retrying?
}

# define the dag 
dag = DAG('tabela4', # give the dag a name 
           schedule_interval='0 0 * * *', # define how often you want it to run - you can pass cron expressions here
           default_args=default_args # pass the default args defined above or you can override them here if you want this dag to behave a little different
         )   
 
bq_task_1 = BigQueryOperator(
        dag = dag, # need to tell airflow that this task belongs to the dag we defined above
        task_id='process_tabela4', # task id's must be uniqe within the dag
        bql='select  linha, year(data_venda) ano, month(data_venda) mes, sum(QTD_VENDA) total from [teste_boticatio.base_boticario] group by 1,2,3 order by 1,2,3', # the actual sql command we want to run on bigquery is in this file in the same folder. it is also templated
        destination_dataset_table='teste_boticatio.tabela4', # we also in this example want our target table to be lob and task specific
        write_disposition='WRITE_TRUNCATE', # drop and recreate this table each time, you could use other options here
        bigquery_conn_id='bigquery_default' # this is the airflow connection to gcp we defined in the front end. More info here: https://github.com/alexvanboxel/airflow-gcp-examples
    )