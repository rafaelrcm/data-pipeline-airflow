from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_statement="",
                 append="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.append = append

    def execute(self, context):
        self.log.info('Loading Dimension')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append == True:
            sql_statement = "INSERT INTO {} {}".format(self.table,self.sql_statement)
            redshift.run(sql_statement)
        else:
            sql_statement = "DELETE FROM {}".format(self.table)
            redshift.run(sql_statement)
            sql_statement = "INSERT INTO {} {}".format(self.table,self.sql_statement)
            redshift.run(sql_statement)
