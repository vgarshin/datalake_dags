import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

from kubernetes.client import models as k8s

with DAG(
    dag_id="example_pod_template_file",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example3"],
) as dag:
    executor_config_template = {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"release": "stable"})
        ),
    }

    @task(executor_config=executor_config_template)
    def task_with_template():
        print_stuff()
