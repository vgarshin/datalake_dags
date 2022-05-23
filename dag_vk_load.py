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

with DAG(dag_id='vk_data_load',
         default_args=default_args,
         start_date=datetime(2022, 5, 23),
         end_date=datetime(2027, 5, 23),
         schedule_interval='0 5 * * 1',
         tags=['vk']) as dag:
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
    task1 = KubernetesPodOperator(
        task_id='vk_data_load_run_script_ma',
        name='vk_load_run_script_ma',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20220204v0',
        resources=resource_req,
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'git clone https://github.com/vgarshin/datalake_scripts && python datalake_scripts/vk_load.py gsom_ma'
        ],
        is_delete_operator_pod=True,
        startup_timeout_seconds=900,
    )
    task2 = KubernetesPodOperator(
        task_id='vk_data_load_run_script_bak',
        name='vk_load_run_script_bak',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20220204v0',
        resources=resource_req,
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'git clone https://github.com/vgarshin/datalake_scripts && python datalake_scripts/vk_load.py gsom_abiturient'
        ],
        is_delete_operator_pod=True,
        startup_timeout_seconds=900,
    )
    task3 = KubernetesPodOperator(
        task_id='vk_data_load_run_script_spbu',
        name='vk_load_run_script_spbu',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20220204v0',
        resources=resource_req,
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'git clone https://github.com/vgarshin/datalake_scripts && python datalake_scripts/vk_load.py gsom.spbu'
        ],
        is_delete_operator_pod=True,
        startup_timeout_seconds=900,
    )

    task
