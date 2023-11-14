import os
import glob
import pandas as pd
import re
import json
from mojimoji import zen_to_han
import pyarrow as pa
import pyarrow.parquet as pq
from modules.gcp_class import Gcs_client, Bigquery_cliant

pa_dict = {"STRING": pa.string(), "FLOAT64": pa.float64()}
ja_to_en = {
    "ひったくり": "hittakuri",
    "自転車盗": "zitensyatou",
    "車上ねらい": "syajyounerai",
    "部品ねらい": "buhinnerai",
    "自動販売機ねらい": "zihankinerai",
    "自動車盗": "zidousyatou",
    "オートバイ盗": "ootobaitou",
}


def add_all_cols(df, cols_list):
    """データフレームにないカラムに欠損を加える。

    Args:
        df (_type_): _description_
        cols_list (_type_): _description_

    Returns:
        _type_: _description_
    """
    no_cols = [c for c in cols_list if c not in df.columns]
    for c in no_cols:
        df[c] = ""
    return df[cols_list]


def clean_data(file, use_cols_dict):
    # データの前処理
    d = pd.read_csv(file, encoding="shift-jis")
    # 全角半角処理
    d.columns = [zen_to_han(c, kana=False) for c in d.columns]
    # 余分な文字列の置換
    d.columns = [re.sub('\(発生地\)|\(始期\)|\\n|"|\[|\]', "", c).strip() for c in d.columns]
    # 値の全角半角処理
    zen_han_cols = ["都道府県", "市区町村", "町丁目"]
    for c in zen_han_cols:
        d[c] = [zen_to_han(v, kana=False) for v in d[c].fillna("").astype(str)]
    # 列名を英字に変換
    d.columns = [use_cols_dict.get(c) for c in d.columns]
    d = add_all_cols(d, list(use_cols_dict.values()))
    # 全てを文字列に
    d = d.astype(str)
    return d


def main():
    with open("./data_meta.json") as f:
        table_meta = json.load(f)

    table_name = "tokyo_crime_data"
    use_cols_meta = [t["cols"] for t in table_meta if t["table_name"] == table_name][0]
    use_cols_dict = {m["ja_name"]: m["en_name"] for m in use_cols_meta}
    # GCSにアップロード
    gcs_client = Gcs_client()
    gcs_client.create_bucket(table_name)
    files = glob.glob("../data/tokyo/*.csv", recursive=True)
    fields = [pa.field(m["en_name"], pa_dict[m["col_type"]]) for m in use_cols_meta]
    table_schema = pa.schema(fields)
    all_objects = gcs_client.list_all_objects(table_name)
    for f in files:
        # データのクレンジング
        d = clean_data(f, use_cols_dict)
        # ファイル名に都道府県名、フォルダ名に手口をつける。
        pref = os.path.dirname(f).split("/")[-1]
        teguchi = ja_to_en[d["teguchi"].values[0]]
        local_file_name = os.path.basename(f.lower()).replace(".csv", "")
        local_file_name = f"{pref}_{local_file_name}.parquet"
        local_path = f"../output/{local_file_name}"
        upload_path = f"teguchi_en={teguchi}/{local_file_name}"
        # すでにファイルがあればスキップ
        if upload_path in set(all_objects):
            continue
        table = pa.Table.from_pandas(d, schema=table_schema, preserve_index=False)
        pq.write_table(table, local_path)
        gcs_client.upload_gcs(table_name, local_path, upload_path)

    # テーブル作成
    bq_client = Bigquery_cliant()
    dataset_name = "crime_dataset"
    table_name = "tokyo_crime_data"
    bq_client.create_dataset(dataset_name)
    schema = bq_client.create_string_schema(use_cols_dict.values())
    table_id = f"{bq_client.client.project}.{dataset_name}.{table_name}"
    bq_client.create_external_table(table_name, table_id, schema, partitioned=True)


if __name__ == "__main__":
    # スクリプトの絶対パスを取得
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # カレントディレクトリをスクリプトのディレクトリに変更
    os.chdir(script_dir)
    main()
