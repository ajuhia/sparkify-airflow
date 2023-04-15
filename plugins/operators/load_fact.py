from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {table}
        {sql_statement}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_statement = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        

    def execute(self, context):
        """
        This function:
        - fetches value from the hooks to connect to DWH.
        - inserts data into the fact table songplays by formatting and executing insert_sql defined above.
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
         
        self.log.info('Load data into Fact table Songplays')
        formatted_sql = LoadFactOperator.insert_sql.format(
                        table=self.table,
                        sql_statement = self.sql_statement)
        
               
        
        redshift.run(formatted_sql)
        
        self.log.info(f"Data insertion completed for {self.table} table!")
