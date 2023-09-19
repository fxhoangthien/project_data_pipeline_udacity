from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,

                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Check for {table} ")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data Import Failed. {table} contained 0 rows")
            self.log.info(f"Run well. {table} check passed with {records[0][0]} records")
            self.log.info('Data Quality Implemented')