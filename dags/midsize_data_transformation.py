from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from tempfile import NamedTemporaryFile
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import os


class WasbXComBackend(BaseXCom):
    PREFIX = "xcom_blob://"
    CONTAINER_NAME = "xcom-backend"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pa.Table):
            # # Create a named temporary file
            with NamedTemporaryFile(mode="wb", delete=False) as temp:
                # Write to the temporary file
                pq.write_table(value, temp)
                # Get the temporary file's name
                print(f"The temporary file's name is: {temp.name}")

                # The temporary file will be deleted when the `with` block is exited

                hook = WasbHook(wasb_conn_id="wasb_docker")
                key = f"data_{str(uuid.uuid4())}.snappy.parquet"
                # filename = f"{key}.csv"

            hook.load_file(
                max_concurrency=8,
                file_path=temp.name,
                container_name=WasbXComBackend.CONTAINER_NAME,
                blob_name=key,
                overwrite=True
            )
            os.unlink(temp.name)
                
            value = WasbXComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(WasbXComBackend.PREFIX):

            hook = WasbHook(wasb_conn_id="wasb_docker")
            key = result.replace(WasbXComBackend.PREFIX, "")
            file_name = f"{key}.csv"

            with NamedTemporaryFile() as temp_file:
                hook.get_file(
                    max_concurrency=8,
                    file_path=temp_file.name,
                    container_name=WasbXComBackend.CONTAINER_NAME,
                    blob_name=key
                )
                result = pq.read_table(temp_file.name)
        return result