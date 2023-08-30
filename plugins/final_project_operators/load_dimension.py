from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id='',
                 sql_create_statement = '',
                 sql_load_statement='',
                 truncate_insert  = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_create_statement = sql_create_statement
        self.sql_load_statement = sql_load_statement
        self.truncate_insert = truncate_insert

    # Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load.
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Create table if not exists
        postgres.run(self.sql_create_statement)

        if self.truncate_insert:
            self.log.info(f'Truncate the {self.table} dimension table')
            postgres.run(f'TRUNCATE {self.table}')
            
        self.log.info(f'Loading data from staging table to a {self.table} dimension')
        postgres.run(f'INSERT INTO {self.table} {self.sql_load_statement}')
