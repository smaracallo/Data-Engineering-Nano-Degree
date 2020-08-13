from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # operators params (with defaults)
                 table="",
                 redshift_conn_id="",
                 insert_sql_stmt="",
                 truncate=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # parameter mapping
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql_stmt = insert_sql_stmt
        self.truncate = truncate

    def execute(self, context):
        """
        Insert data into dimensional tables from staging events and song data tables
        using sql truncate method to empty target tables prior to load.
        """

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into dimension table '{self.table}' in Redshift")

        if self.truncate:
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        table_insert_sql = f"""
            INSERT INTO {self.table}
            ({self.insert_sql_stmt})
        """

        redshift_hook.run(table_insert_sql)
        self.log.info("Successfully loaded data into dimension tables")
