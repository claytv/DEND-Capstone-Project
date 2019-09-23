from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadTableOperator(BaseOperator):
    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {} {} 
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 columns_sql='',
                 delete = 'False',
                 sql="",
                 *args, **kwargs):
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        delete
        self.columns_sql = columns_sql
        self.delete = delete
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete:
            self.log.info("Clearing data from destination table")
            redshift.run("DELETE FROM {}".format(self.table))

        insert_query = LoadTableOperator.insert_sql.format(
            self.table,
            self.columns_sql,
            self.sql
        )
        self.log.info("Loading data into destination table")
        redshift.run(insert_query)
