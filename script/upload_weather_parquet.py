import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from modules.gcp_class import Gcs_client, Bigquery_cliant

pa_dict = {"STRING": pa.string(), "FLOAT64": pa.float64()}


def main():
    with open("./data_meta.json") as f:
        table_meta = json.load(f)

    table_name = "daily_weathers"
    use_cols_meta = [t["cols"] for t in table_meta if t["table_name"] == table_name][0]
    use_cols_dict = {m["ja_name"]: m["en_name"] for m in use_cols_meta}
    df = pd.read_csv(
        "../data/weather/tokyo_weather.csv",
        encoding="shift-jis",
        header=2,
        usecols=use_cols_dict.keys(),
    ).rename(columns=use_cols_dict)
    df = df[df["weather_date"].notna()]

    # GCSにアップロード
    gcs_client = Gcs_client()
    gcs_client.create_bucket(table_name)
    fields = [pa.field(m["en_name"], pa_dict[m["col_type"]]) for m in use_cols_meta]
    table_schema = pa.schema(fields)
    local_path = f"../output/weather_data"
    table = pa.Table.from_pandas(df, schema=table_schema, preserve_index=False)
    pq.write_table(table, local_path)
    gcs_client.upload_gcs(table_name, local_path, table_name)

    # テーブル作成
    bq_client = Bigquery_cliant()
    dataset_name = "crime_dataset"
    bq_client.create_dataset(dataset_name)
    schema_types = {m["en_name"]: m["col_type"] for m in use_cols_meta}
    schema = bq_client.create_schema(schema_types)
    table_id = f"{bq_client.client.project}.{dataset_name}.{table_name}"
    bq_client.create_external_table(table_name, table_id, schema, partitioned=False)


if __name__ == "__main__":
    # スクリプトの絶対パスを取得
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # カレントディレクトリをスクリプトのディレクトリに変更
    os.chdir(script_dir)
    main()
