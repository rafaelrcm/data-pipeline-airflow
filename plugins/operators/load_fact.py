from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info('Loading Fact')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_statement = "INSERT INTO {} {}".format(self.table,self.sql_statement)
        redshift.run(sql_statement)