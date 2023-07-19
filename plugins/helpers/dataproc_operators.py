from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator,
)


class DataprocTasks:
    "This class configures airflow tasks to use the GCP dataproc service"

    def __init__(
        self,
        cluster_namme: str,
        region: str,
        project_id: str,
        cluster_config: Optional[str] = None,
        jars: Optional[List[str]] = None,
    ) -> None:
        self.cluster_name = cluster_namme
        self.region = region
        self.project_id = project_id
        if cluster_config:
            self.cluster_config
        else:
            self.cluster_config = {
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n2-standard-2",
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": 512,
                    },
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n2-standard-2",
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": 512,
                    },
                },
            }
        self.jars = jars

    def create_cluster_task(self) -> DataprocCreateClusterOperator:
        return DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=self.project_id,
            cluster_config=self.cluster_config,
            region=self.region,
            cluster_name=self.cluster_name,
        )

    def delete_cluster_task(self) -> DataprocDeleteClusterOperator:
        return DataprocDeleteClusterOperator(
            task_id="delete_cluster",
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
        )

    def create_pyspark_task(
        self,
        task_id: str,
        main_python_file_uri: str,
        pyfiles: Optional[List[str]] = None,
        arguments: Optional[List[str]] = None,
    ) -> DataprocSubmitPySparkJobOperator:
        return DataprocSubmitPySparkJobOperator(
            task_id=task_id,
            main=main_python_file_uri,
            arguments=arguments,
            pyfiles=pyfiles,
            job_name=f"{task_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{uuid4().hex}",
            cluster_name=self.cluster_name,
            region=self.region,
            dataproc_jars=self.jars,
            retries=3,
        )
