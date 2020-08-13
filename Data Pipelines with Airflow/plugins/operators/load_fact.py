from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # operators params (with defaults)
                 table="",
                 redshift_conn_id="",
                 insert_sql_stmt="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # parameter mapping

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql_stmt = insert_sql_stmt

    def execute(self, context):
        """
        Insert records data into fact table from both staging events and songs tables.
        """

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into fact table in Redshift")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            ({self.insert_sql_stmt})
        """
        redshift_hook.run(table_insert_sql)
