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

with DAG(dag_id='monkey_data_load',
         default_args=default_args,
         start_date=datetime(2022, 3, 1),
         end_date=datetime(2027, 3, 1),
         schedule_interval='30 19 * * *',
         tags=['monkey']) as dag:
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
        task_id='monkey_data_load_run_script',
        name='monkey_load_run_script',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20220204v0',
        resources=resource_req,
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'git clone https://github.com/vgarshin/datalake_scripts && python datalake_scripts/monkey_load.py 1'
        ],
        is_delete_operator_pod=True,
        startup_timeout_seconds=900,
    )

    task
