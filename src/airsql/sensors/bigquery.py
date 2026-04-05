from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.sensors.sql import SqlSensor


class BigQuerySqlSensor(SqlSensor):
    """BigQuery SQL sensor with retry logic.

    This sensor executes a SQL query against BigQuery and succeeds when the
    query returns any rows. It includes retry logic and will skip the task
    if the poke fails after the specified number of retries.

    Args:
        conn_id: The BigQuery connection ID to use.
        location: BigQuery location. Defaults to 'us-central1'.
        retries: Number of retries before skipping the task. Defaults to 1.
        **kwargs: Additional arguments passed to SqlSensor.
    """

    def __init__(self, *, retries=1, location: str = 'us-central1', **kwargs):
        super().__init__(**kwargs)
        self.location = location
        self.poke_count = 0
        self.retries = retries

    def poke(self, context):
        """Execute the sensor check.

        Args:
            context: Airflow task context.

        Returns:
            True if the query returns rows, False otherwise.

        Raises:
            AirflowSkipException: If poke returns False after retries are exhausted.
        """
        self.poke_count += 1
        super_poke = super().poke(context)
        retries = self.retries if isinstance(self.retries, int) else 0
        if not super_poke and self.poke_count > retries:
            raise AirflowSkipException('Skipping task because poke returned False.')
        return super_poke

    def _get_hook(self, location='us-central1'):
        """Get the BigQuery hook for the sensor.

        Args:
            location: BigQuery location. Defaults to 'us-central1'.

        Returns:
            BigQueryHook instance for the configured connection.
        """
        from airflow.providers.google.cloud.hooks.bigquery import (  # noqa: PLC0415
            BigQueryHook,  # noqa: PLC0415
        )

        return BigQueryHook(
            gcp_conn_id=self.conn_id,
            use_legacy_sql=False,
            location=self.location or location,
        )
