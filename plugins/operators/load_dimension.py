from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.mode = mode
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Mode set to {}".format(self.mode))
        if self.mode == "delete-load":
            self.log.info('Clearing dimension table {}...'.format(self.table))
            redshift_hook.run("TRUNCATE TABLE {}".format(self.table))    
            self.log.info('Clearing complete')

        self.log.info('Loading data to dimension {} table...'.format(self.table))
        redshift_hook.run("INSERT INTO {} {}".format(self.table, self.sql))
        self.log.info('Import to dimension {} complete'.format(self.table))