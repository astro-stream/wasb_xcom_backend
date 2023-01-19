FROM quay.io/astronomer/astro-runtime:7.1.0

ENV AIRFLOW__SECRETS__BACKEND=airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
ENV AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "vault_url": "${KEY_VAULT_NAME}"}'
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__CORE__XCOM_BACKEND=include.blob_xcom_backend.WasbXComBackend