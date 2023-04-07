from airflow.plugins_manager import AirflowPlugin
from operators.exec_query_xcom_operator import *


class ExecQueryXcomPlugin(AirflowPlugin):
                    
    name = 'ExecQueryXcomPlugin'
                    
    hooks = []
    operators = [ExecQueryXcomOperator]
    sensors = []