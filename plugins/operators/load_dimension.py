from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {table}
        {sql_statement}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_statement = '',
                 append_row = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.append_row = append_row

    def execute(self, context):
        """
        This function:
        - fetches value from the hooks to connect to DWH.
        - inserts data into the dimension tables by formatting and executing insert_sql defined above.
        """
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Delete data from dimension table")
        if not self.append_row:
            redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info('Load data into passed dimension table')
        formatted_sql = LoadDimensionOperator.insert_sql.format(
                        table = self.table,
                        sql_statement = self.sql_statement
        )
             
        redshift.run(formatted_sql)
        
        self.log.info(f"Data insertion completed for {self.table} table!")
