from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class BigQuerySqlSensor(SqlSensor):
    def __init__(self, *, retries=1, location: str = 'us-central1', **kwargs):
        super().__init__(**kwargs)
        self.location = location
        self.poke_count = 0
        self.retries = retries

    def poke(self, context):
        self.poke_count += 1
        super_poke = super().poke(context)
        if not super_poke and self.poke_count > self.retries:
            raise AirflowSkipException('Skipping task because poke returned False.')
        return super_poke

    def _get_hook(self, location='us-central1') -> BigQueryHook:
        return BigQueryHook(
            gcp_conn_id=self.conn_id,
            use_legacy_sql=False,
            location=self.location or location,
        )
