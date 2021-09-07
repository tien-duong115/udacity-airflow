from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate = """
        BEGIN;
        TRUNCATE TABLE {};
        COMMIT;
    """

    create_dim = """
        INSERT INTO {}
        {};
    """
    @apply_defaults
    def __init__(self,
                 table_name="",
                 redshift_conn_id="",
                 pay_load="",
                 truncate_table=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name=table_name
        self.redshift_conn_id=redshift_conn_id
        self.pay_load=pay_load
        self.truncate_table=truncate_table
        
    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"truncating dimension '{self.table_name}' table")
            redshift.run(LoadDimensionOperator.truncate.format(self.table_name))


        self.log.info(f"Inserting dimension into '{self.table_name}' table")
        normal = LoadDimensionOperator.create_dim.format(self.table_name, self.pay_load)
        redshift.run(normal)

        