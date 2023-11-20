import streamlit as st
import json
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class Gcs_client:
    def __init__(self) -> None:
        self.credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
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
        self.credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
            location="US-WEST1",
        )

    @staticmethod
    def create_schema(schema_dict):
        schema = []
        for k, v in schema_dict.items():
            schema.append(bigquery.SchemaField(k, v, mode="NULLABLE"))

        return schema

    def read_sql(self, query):
        df = self.client.query(query).to_dataframe()
        return df

    def create_dataset(self, dataset_id):
        # データセットの設定
        dataset = bigquery.Dataset(f"{self.client.project}.{dataset_id}")
        dataset.location = "US-WEST1"  # データセットのロケーションを選択

        # データセットの作成
        try:
            self.client.get_dataset(dataset_id)  # APIリクエストを使ってデータセットを取得
            print(f"Dataset already exists: {dataset_id}")
        except NotFound:
            # データセットが存在しない場合、新しいデータセットを作成
            dataset = self.client.create_dataset(dataset, timeout=30)  # タイムアウトを30秒に設定
            print(f"Created dataset {self.client.project}.{dataset_id}")

    def create_external_table(self, bucket_name, table_id, schema, partitioned=False):
        url = f"gs://{bucket_name}/*"
        source_url_prefix = f"gs://{bucket_name}"

        # Configure the external data source.
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [url]
        external_config.autodetect = True

        # Configure partitioning options.
        # If partitioned is True, configure partitioning options.
        if partitioned:
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
