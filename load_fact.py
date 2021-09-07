from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    formatted_sql = """
        INSERT INTO {}
        {};
    """
    @apply_defaults
    def __init__(self,
        table_name="",
        redshift_conn_id="",
        pay_load="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id=redshift_conn_id
        self.pay_load=pay_load

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.formatted_sql.format(
            self.table_name,
            self.pay_load
        )
        self.log.info(f"loading fact table '{self.table_name}' into Redshift")

        redshift.run(formatted_sql)
        self.log.info(f"Finished Loading '{self.table_name}' into Redshift")