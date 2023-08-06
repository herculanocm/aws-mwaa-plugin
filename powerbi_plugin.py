from airflow.plugins_manager import AirflowPlugin
from hooks.powerbi_client_credentials_hook import PowerBIClientCredentialsHook
from operators.powerbi_dataflow_refresh_operator import PowerBIDataflowRefreshOperator


class PowerbiPlugin(AirflowPlugin):
                    
    name = 'PowerbiPlugin'
                    
    hooks = [PowerBIClientCredentialsHook]
    operators = [PowerBIDataflowRefreshOperator]
    sensors = []