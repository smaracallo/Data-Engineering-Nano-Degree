from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # operators params (with defaults)
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # parameter mapping
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Perform data quality checks on dimension tables records.
        """

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            sql_select = f"SELECT COUNT(*) FROM public.{table['name']} WHERE {table['column']} IS NULL"

            records_count = redshift_hook.get_records(sql=sql_select)[0][0]

            if records_count > 0:
                raise ValueError(f"Data quality records check failed. {table['name']} returned null columns")
            self.log.info(f"Data quality on table {table['name']} check passed")
