from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 verify=None,
                 table="",
                 s3_path="",
                 copy_options="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_credentials_id
        self.verify = verify
        self.table = table
        self.s3_path = s3_path
        self.copy_options = copy_options

    def execute(self, context):
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        credentials = self.s3.get_credentials()

        self.log.info("Clearing data")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying {} data from S3 to Redshift".format(self.table))
        copy_query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_options
        )
        redshift.run(copy_query)




