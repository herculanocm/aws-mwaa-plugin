from airflow.plugins_manager import AirflowPlugin
from operators.get_last_dateutil_operator import *


class GetLastDateUtilPlugin(AirflowPlugin):
                    
    name = 'GetLastDateUtilPlugin'
                    
    hooks = []
    operators = [GetLastDateUtilOperator]
    sensors = []