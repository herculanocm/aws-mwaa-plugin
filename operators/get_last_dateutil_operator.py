from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import closing

class GetLastDateUtilOperator(BaseOperator):

    def __init__(self, 
        sql, 
        redshift_conn_id,
        xcom_task_id_key,
        *args, **kwargs):
        super(GetLastDateUtilOperator, self).__init__(*args, **kwargs)

        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.xcom_task_id_key = xcom_task_id_key

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        print('Executando consulta no banco')
        result = []
        with closing(pg.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.itersize = 1000
                cur.execute(self.sql)

                for row in cur:
                    result.append(row)

        
        task_instance = context['task_instance']
        print('Setando xcom')
        task_instance.xcom_push(self.xcom_task_id_key, result)

        