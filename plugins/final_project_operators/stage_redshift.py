from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    copy_sql = """ COPY {} \
        FROM '{}' \
        ACCESS_KEY_ID '{}' \
        SECRET_ACCESS_KEY '{}' \
        REGION '{}' \
        JSON '{}'
    """

    ui_color = '#358140'

    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 json_format ='',
                 sql_create_statement = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_format = json_format
        self.sql_create_statement = sql_create_statement
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # create table if not exists
        postgres.run(self.sql_create_statement)

        self.log.info('Clearing data from destination table ...')
        postgres.run(f'TRUNCATE {self.table}')

        self.log.info('Copying data from S3 to Redshift ...')
        rendered_key = self.s3_key.format(**context)
        print('The render key: ',rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            rendered_key,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
        print('formatted_sql: ' , formatted_sql)
        postgres.run(formatted_sql)