import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

from kubernetes.client import models as k8s

from airflow.configuration import conf

worker_container_repository = conf.get('kubernetes', 'worker_container_repository')
worker_container_tag = conf.get('kubernetes', 'worker_container_tag')

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

    # ================================================================

    @task(executor_config=executor_config_template)
    def task_with_template():
        print_stuff()

    first_task = task_with_template()

    # ================================================================
    # [START task_with_volume]

    executor_config_volume_mount = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[
                            k8s.V1VolumeMount(mount_path="/foo/", name="airflow-volume")
                            #k8s.V1VolumeMount(mount_path="/foo/", name="example-kubernetes-test-volume")
                        ],
                    )
                ],
                volumes=[
                    k8s.V1Volume(
                        name="airflow-volume",
                        #name="example-kubernetes-test-volume",
                        #host_path=k8s.V1HostPathVolumeSource(path="/tmp/"),
                        persistentVolumeClaim={'claimName': "airflow-pvc"}
                    )
                ],
            )
        ),
    }

    @task(executor_config=executor_config_volume_mount)
    def test_volume_mount():
        """
        Tests whether the volume has been mounted.
        """
        with open('/foo/volume_mount_test.txt', 'w') as foo:
            foo.write('Hello')

        return_code = os.system("cat /foo/volume_mount_test.txt")
        if return_code != 0:
            raise ValueError(f"Error when checking volume mount. Return code {return_code}")

    volume_task = test_volume_mount()

    # [END task_with_volume]
    # ================================================================

    # You can also change the base image, here we used the worker image for demonstration.
    # Note that the image must have the same configuration as the
    # worker image. Could be that you want to run this task in a special docker image that has a zip
    # library built-in. You build the special docker image on top your worker image.
    kube_exec_config_special = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        #name="base", image=f"{worker_container_repository}:{worker_container_tag}"
                        name="sparketl", image="vgarshin/mibapysparks3:20211002v1"
                  ),
                ]
            )
        )
    }

    @task(executor_config=kube_exec_config_special)
    def base_image_override_task():
        print_stuff()

    second_task = base_image_override_task()

    # ================================================================

    first_task >> volume_task >> second_task
