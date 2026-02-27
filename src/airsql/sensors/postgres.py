from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresSqlSensor(SqlSensor):
    """PostgreSQL SQL sensor with retry logic.

    This sensor executes a SQL query and succeeds when the query returns any
    rows. It includes retry logic and will skip the task if the poke fails
    after the specified number of retries.

    Args:
        conn_id: The PostgreSQL connection ID to use. Defaults to 'postgres_default'.
        retries: Number of retries before skipping the task. Defaults to 1.
        **kwargs: Additional arguments passed to SqlSensor.
    """

    def __init__(self, *, retries=1, conn_id='postgres_default', **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)
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
        if not super_poke and self.poke_count > self.retries:
            raise AirflowSkipException('Skipping task because poke returned False.')
        return super_poke

    def _get_hook(self) -> PostgresHook:
        """Get the PostgreSQL hook for the sensor.

        Returns:
            PostgresHook instance for the configured connection.
        """
        return PostgresHook(postgres_conn_id=self.conn_id)
