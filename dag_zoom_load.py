from kubernetes.client import V1VolumeMount, V1Volume, V1PersistentVolumeClaimVolumeSource

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

with DAG(dag_id='zoom_data_load',
         default_args=default_args,
         start_date=datetime(2022, 1, 28),
         end_date=datetime(2027, 1, 28),
         schedule_interval='30 6 * * *',
         tags=['zoom']) as dag:
    air_volume = V1Volume(
        name='airflow-volume',
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name='airflow-pvc')
    )
    air_volume_mount = V1VolumeMount(
        mount_path='/home/jovyan/zoomdataload',
        name='airflow-volume'
    )
    task = KubernetesPodOperator(
        task_id='zoom_data_load_run_script',
        name='zoom_run_script',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20211002v1',
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'git clone https://github.com/vgarshin/datalake_scripts && mv datalake_scripts/*.py /home/jovyan/zoomdataload && cd /home/jovyan/zoomdataload && python zoom_load.py > logs/zoom_load_log.txt'
        ],
        startup_timeout_seconds=300,
    )

    task
