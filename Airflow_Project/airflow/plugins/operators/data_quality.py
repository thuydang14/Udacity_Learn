from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 param = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.param = param

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Count data inserted to DATABASE') 
        for stmt in self.param:
            if (stmt['tables']):
                for table in stmt['tables']:
                    result = int(redshift.get_first(sql=stmt['sql'].format(table))[0])
                    # check if equal
                    if stmt['op'] == 'eq':
                        if result != stmt['val']:
                            raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
                    # check if not equal
                    elif stmt['op'] == 'ne':
                        if result == stmt['val']:
                            raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
                    # check if greater than
                    elif stmt['op'] == 'gt':
                        if result <= stmt['val']:
                            raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
                    self.log.info(f"Passed check: {result} {stmt['op']} {stmt['val']}")
            
            else:
                result = int(redshift.get_first(sql=stmt['sql'])[0])
                # check if equal
                if stmt['op'] == 'eq':
                    if result != stmt['val']:
                        raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
                        # check if not equal
                elif stmt['op'] == 'ne':
                    if result == stmt['val']:
                        raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
                            # check if greater than
                elif stmt['op'] == 'gt':
                    if result <= stmt['val']:
                        raise AssertionError(f"Check failed: {result} {stmt['op']} {stmt['val']}")
            self.log.info(f"Passed check: {result} {stmt['op']} {stmt['val']}")
            