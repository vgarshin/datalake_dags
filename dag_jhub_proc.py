from kubernetes.client import (
    V1VolumeMount, 
    V1Volume, 
    V1PersistentVolumeClaimVolumeSource, 
    V1ResourceRequirements
)
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['vgarshin@vtb.education'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
resource_req = V1ResourceRequirements(
    requests={
        'cpu': 1,
        'memory': '2G'
    },
    limits={
        'cpu': 2,
        'memory': '4G'
    }
)

with DAG(dag_id='jhub_data_proc',
         default_args=default_args,
         start_date=datetime(2022, 2, 22, 10, 0, 0),
         end_date=datetime(2027, 2, 22),
         schedule_interval='10 * * * *',
         tags=['jhub']) as dag:
    air_volume = V1Volume(
        name='airflow-volume',
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
            claim_name='airflow-shared-pvc'
        )
    )
    air_volume_mount = V1VolumeMount(
        mount_path='/home/jovyan/zoomdataload',
        name='airflow-volume'
    )
    task = KubernetesPodOperator(
        task_id='jhub_data_proc_run_script',
        name='jhub_proc_run_script',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20220204v0',
        resources=resource_req,
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'git clone https://github.com/vgarshin/datalake_scripts && export SPARK_LOCAL_HOSTNAME=localhost && python datalake_scripts/jhub_proc.py'
        ],
        is_delete_operator_pod=True,
        startup_timeout_seconds=900,
    )

    task
