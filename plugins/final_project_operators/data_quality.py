import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, checks=[], redshift_conn_id = '', *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.checks:
            test_sql = check.get('test_sql')
            expected_result = check.get('expected_result')
            comparison = check.get('comparison')

            actual_result = redshift_hook.get_records(test_sql)[0][0]
            print('test_sql', test_sql)
            print('expected_result', expected_result)
            print('comparison', comparison)
            print('actual_result' , actual_result)

            if comparison == '==':
                if actual_result == expected_result:
                    logging.info('Data quality check passed.') 
                else:
                    raise ValueError(f'Data quality check failed.')
            elif comparison == '!=':
                if actual_result != expected_result:
                    logging.info('Data quality check passed.') 
                else:
                    raise ValueError(f'Data quality check failed.')
            elif comparison == '>':
                if actual_result > expected_result:
                    logging.info('Data quality check passed.') 
                else:
                    raise ValueError(f'Data quality check failed.')
            elif comparison == '>=':
                if actual_result != expected_result:
                    logging.info('Data quality check passed.') 
                else:
                    raise ValueError(f'Data quality check failed.')
            elif comparison == '<':
                if actual_result != expected_result:
                    logging.info('Data quality check passed.') 
                else:
                    raise ValueError(f'Data quality check failed.')
            elif comparison == '<=':
                if actual_result != expected_result:
                    logging.info('Data quality check passed.') 
                else:
                    raise ValueError(f'Data quality check failed.')
            else:
                print("Invalid comparison operator")