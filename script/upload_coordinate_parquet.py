import os
import pandas as pd
from mojimoji import zen_to_han
import pyarrow as pa
import pyarrow.parquet as pq
from modules.gcp_class import Gcs_client, Bigquery_cliant

BUCKET_NAME = "crime_vis_master"


def main():
    # データを読む
    use_cols_meta = [
        {
            "ja_name": "都道府県名",
            "en_name": "prefecture",
            "pa_type": pa.string(),
            "col_type": "STRING",
        },
        {
            "ja_name": "市区町村名",
            "en_name": "city",
            "pa_type": pa.string(),
            "col_type": "STRING",
        },
        {
            "ja_name": "大字・丁目名",
            "en_name": "cyoume",
            "pa_type": pa.string(),
            "col_type": "STRING",
        },
        {
            "ja_name": "緯度",
            "en_name": "longitude",
            "pa_type": pa.float64(),
            "col_type": "FLOAT64",
        },
        {
            "ja_name": "経度",
            "en_name": "latitude",
            "pa_type": pa.float64(),
            "col_type": "FLOAT64",
        },
    ]
    use_cols_dict = {m["ja_name"]: m["en_name"] for m in use_cols_meta}
    df = pd.read_csv(
        "../data/master/13_2022.csv", encoding="shift-jis", usecols=use_cols_dict.keys()
    ).rename(columns=use_cols_dict)

    # GCSにアップロード
    gcs_client = Gcs_client()
    gcs_client.create_bucket(BUCKET_NAME)
    fields = [pa.field(m["en_name"], m["pa_type"]) for m in use_cols_meta]
    table_schema = pa.schema(fields)
    local_path = f"../output/coordinate_master"
    upload_path = f"coordinate_master"
    table = pa.Table.from_pandas(df, schema=table_schema, preserve_index=False)
    pq.write_table(table, local_path)
    gcs_client.upload_gcs(BUCKET_NAME, local_path, upload_path)

    # テーブル作成
    bq_client = Bigquery_cliant()
    dataset_name = "crime_dataset"
    table_name = "coordinate_master"
    bq_client.create_dataset(dataset_name)
    schema_types = {m["en_name"]: m["col_type"] for m in use_cols_meta}
    schema = bq_client.create_schema(schema_types)
    table_id = f"{bq_client.client.project}.{dataset_name}.{table_name}"
    bq_client.create_external_table(BUCKET_NAME, table_id, schema, partitioned=False)


if __name__ == "__main__":
    # スクリプトの絶対パスを取得
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # カレントディレクトリをスクリプトのディレクトリに変更
    os.chdir(script_dir)
    main()
