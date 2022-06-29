from kubernetes.client import (
    V1VolumeMount, 
    V1Volume, 
    V1PersistentVolumeClaimVolumeSource, 
    V1ResourceRequirements
)
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret
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
secret_env_user= Secret(
    deploy_type='env',
    deploy_target='GITLAB_USERNAME',
    secret='gitlab-credentials',
    key='GITLAB_USERNAME'
)
secret_env_pwd= Secret(
    deploy_type='env',
    deploy_target='GITLAB_PASSWORD',
    secret='gitlab-credentials',
    key='GITLAB_PASSWORD'
)
YESTERDAY = datetime.now() - timedelta(days=1)

with DAG(dag_id='vk_data_load_test',
         default_args=default_args,
         start_date=YESTERDAY,
         end_date=YESTERDAY + timedelta(days=365),
         schedule_interval='0 5 * * 1',
         tags=['vk']) as dag:
    air_volume = V1Volume(
        name='airflow-volume',
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
            claim_name='airflow-shared-pvc'
        )
    )
    air_volume_mount = V1VolumeMount(
        mount_path='/home/jovyan/dataload',
        name='airflow-volume'
    )
    task1 = KubernetesPodOperator(
        task_id='vk_data_load_run_script_ma_test',
        name='vk_load_run_script_ma_test',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20220204v0',
        resources=resource_req,
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'echo $GITLAB_USERNAME && echo $GITLAB_PASSWORD && git clone https://gitlab.spbu.ru/st807908/digital-profile-student.git'
        ],
        is_delete_operator_pod=True,
        startup_timeout_seconds=900,
        secrets=[secret_env_user, secret_env_pwd, ],
    )

    task1
