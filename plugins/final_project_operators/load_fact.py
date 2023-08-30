from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id='',
                 sql_create_statement='',
                 sql_load_statement = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_create_statement = sql_create_statement
        self.sql_load_statement = sql_load_statement

    # Fact tables are usually so massive that they should only allow append type functionality.
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id) 

        # Create table if not exists
        postgres.run(self.sql_create_statement)


        self.log.info(f'Loading data from staging table to {self.table} fact table')
        postgres.run(f'INSERT INTO {self.table} {self.sql_load_statement}')