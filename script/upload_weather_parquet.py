import os
import re
import pandas as pd
from mojimoji import zen_to_han
import pyarrow as pa
import pyarrow.parquet as pq
from modules.gcp_class import Gcs_client, Bigquery_cliant

BUCKET_NAME = "daily_weather"


def main():
    # データを読む
    use_cols_meta = [
        {
            "ja_name": "年月日",
            "en_name": "date",
            "pa_type": pa.string(),
            "col_type": "STRING",
        },
        {
            "ja_name": "平均気温(℃)",
            "en_name": "average_temperature",
            "pa_type": pa.float64(),
            "col_type": "FLOAT64",
        },
        {
            "ja_name": "最高気温(℃)",
            "en_name": "maximum_temperature",
            "pa_type": pa.float64(),
            "col_type": "FLOAT64",
        },
        {
            "ja_name": "最低気温(℃)",
            "en_name": "mimum_temperature",
            "pa_type": pa.float64(),
            "col_type": "FLOAT64",
        },
        {
            "ja_name": "降水量の合計(mm)",
            "en_name": "precipitation",
            "pa_type": pa.float64(),
            "col_type": "FLOAT64",
        },
        {
            "ja_name": "天気概況(昼：06時〜18時)",
            "en_name": "weather_noon",
            "pa_type": pa.string(),
            "col_type": "STRING",
        },
        {
            "ja_name": "天気概況(夜：18時〜翌日06時)",
            "en_name": "weather_night",
            "pa_type": pa.string(),
            "col_type": "STRING",
        },
    ]

    use_cols_dict = {m["ja_name"]: m["en_name"] for m in use_cols_meta}
    df = pd.read_csv(
        "../data/weather/tokyo_weather.csv",
        encoding="shift-jis",
        header=2,
        usecols=use_cols_dict.keys(),
    ).rename(columns=use_cols_dict)
    df = df[df["date"].notna()]

    # GCSにアップロード
    gcs_client = Gcs_client()
    gcs_client.create_bucket(BUCKET_NAME)
    fields = [pa.field(m["en_name"], m["pa_type"]) for m in use_cols_meta]
    table_schema = pa.schema(fields)
    local_path = f"../output/weather_data"
    upload_path = f"weather_data"
    table = pa.Table.from_pandas(df, schema=table_schema, preserve_index=False)
    pq.write_table(table, local_path)
    gcs_client.upload_gcs(BUCKET_NAME, local_path, upload_path)

    # テーブル作成
    bq_client = Bigquery_cliant()
    dataset_name = "crime_dataset"
    table_name = "daily_weather"
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
