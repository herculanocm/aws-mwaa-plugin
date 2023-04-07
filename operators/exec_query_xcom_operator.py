from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from contextlib import closing
import json

class ExecQueryXcomOperator(BaseOperator):

    template_fields = ("sql", "xcom_task_id_key", "date_execution")

    def __init__(self, 
        sql, 
        redshift_conn_id,
        xcom_task_id_key,
        date_execution,
        *args, **kwargs):
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.xcom_task_id_key = xcom_task_id_key
        self.date_execution = date_execution
        super(ExecQueryXcomOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        rd = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        self.sql = self.sql.format(date_execution = self.date_execution)

        print(f'SQL: {self.sql}')
        print('Executando consulta no banco')
        result_xcom = {
            'values': []
        }
        with closing(rd.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(self.sql)

                for row in cur:
                    result_xcom['values'].append(row)
        
        task_instance = context['task_instance']
        print(f'Values: {result_xcom}')
        print(f'Setando xcom {self.xcom_task_id_key}')
        task_instance.xcom_push(self.xcom_task_id_key, json.dumps(result_xcom))

        