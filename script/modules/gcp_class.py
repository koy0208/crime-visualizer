import json
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery


class Gcs_client:
    def __init__(self) -> None:
        key_path = "./credential.json"
        service_account_info = json.load(open(key_path))
        self.credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        self.client = storage.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
        )

    def create_bucket(self, bucket_name):
        """GCSにバケットがなければ作成する。

        Args:
            bucket_name (_type_): _description_
        """

        if self.client.bucket(bucket_name).exists():
            print(f"already exists {bucket_name}")
        else:
            print(f"create {bucket_name}")
            self.client.create_bucket(bucket_name, location="US-WEST1")

    def list_all_objects(self, bucket_name):
        """バケットの中身をリストで出力する。

        Args:
            bucket_name (_type_): _description_

        Returns:
            _type_: _description_
        """
        blobs = self.client.list_blobs(bucket_name)
        all_objects = [blob.name for blob in blobs]
        return all_objects

    def upload_gcs(self, bucket_name, from_path, to_path, dry_run=False):
        """GSCにファイルをアップロードする。

        Args:
            bucket_name (_type_): _description_
            from_path (_type_): _description_
            to_path (_type_): _description_
            dry_run (bool, optional): _description_. Defaults to False.
        """
        print(f"{from_path} to {bucket_name}/{to_path}")
        if dry_run:
            pass
        else:
            bucket = self.client.get_bucket(bucket_name)
            blob_gcs = bucket.blob(to_path)
            # ローカルのファイルパスを指定
            blob_gcs.upload_from_filename(from_path)


class Bigquery_cliant:
    def __init__(self) -> None:
        key_path = "./credential.json"
        service_account_info = json.load(open(key_path))
        self.credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
            location="US-WEST1",
        )

    @staticmethod
    def create_string_schema(values):
        schema = []
        for c in values:
            schema.append(bigquery.SchemaField(c, "STRING", mode="NULLABLE"))

        return schema

    def read_sql(self, query):
        df = self.client.query(query).to_dataframe()
        return df

    def create_external_table(self, table_id, bucket_name, schema):
        url = f"gs://{bucket_name}/*"
        source_url_prefix = f"gs://{bucket_name}"

        # Configure the external data source.
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [url]
        external_config.autodetect = True

        # Configure partitioning options.
        hive_partitioning_opts = bigquery.HivePartitioningOptions()
        hive_partitioning_opts.mode = "AUTO"
        hive_partitioning_opts.require_partition_filter = False
        hive_partitioning_opts.source_uri_prefix = source_url_prefix
        external_config.hive_partitioning = hive_partitioning_opts

        table = bigquery.Table(table_id, schema=schema)
        table.external_data_configuration = external_config

        self.client.delete_table(table_id, not_found_ok=True)
        table = self.client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    def create_tabel(table_id, bucket_name):
        # テーブル作成
        bq_cliant = Bigquery_cliant()
        table_id = "crimes-porttal.portal_dataset.crimes"

        schema = []
        for c in cols_dict.values():
            schema.append(bigquery.SchemaField(c, "STRING", mode="NULLABLE"))

        bq_cliant.create_external_table(table_id, url, source_url_prefix, schema)
