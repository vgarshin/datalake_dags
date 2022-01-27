from kubernetes.client import V1VolumeMount, V1Volume, V1PersistentVolumeClaimVolumeSource

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

with DAG(dag_id='zoom_data_load',
         start_date=days_ago(1),
         schedule_interval='@once',
         tags=['zoom']) as dag:
    air_volume = V1Volume(
        name='airflow-volume',
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name='airflow-pvc')
    )
    air_volume_mount = V1VolumeMount(
        mount_path='/root/zoomdataload',
        name='airflow-volume'
    )
    task1 = KubernetesPodOperator(
        task_id='zoom_data_load_get_script',
        name='zoom_get_script',
        namespace='airflow',
        image='vgarshin/mibaminpy:20210917v0',
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'date > /root/zoomdataload/date.txt;',
            'git clone https://github.com/vgarshin/datalake_scripts /root/zoomdataload/scripts',
        ],
        startup_timeout_seconds=300,
    )
    task2 = KubernetesPodOperator(
        task_id='zoom_data_load_run_script',
        name='zoom_run_script',
        namespace='airflow',
        image='vgarshin/mibapysparks3:20211002v1',
        volumes=[air_volume, ],
        volume_mounts=[air_volume_mount, ],
        cmds=[
            "sh", "-c",
            'python /root/zoomdataload/scripts/zoom_load.py',
        ],
        startup_timeout_seconds=300,
    )

    task1 >> task2
