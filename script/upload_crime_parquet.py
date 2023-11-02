import os
import glob
import pandas as pd
import re
from mojimoji import zen_to_han
import pyarrow as pa
import pyarrow.parquet as pq
from modules.gcp_class import Gcs_client, Bigquery_cliant

BUCKET_NAME = "crime_board_data"

cols_dict = {
    "罪名": "zaimei",
    "手口": "teguchi",
    "管轄警察署": "keisatsusyo",
    "管轄交番・駐在所": "kouban",
    "都道府県": "prefecture",
    "市区町村コード": "city_code",
    "市区町村": "city",
    "町丁目": "cyoume",
    "発生年月日": "occurrence_day",
    "発生時": "occurrence_time",
    "発生場所": "occurrence_point",
    "発生場所の詳細": "occurrence_point_info",
    "被害者の性別": "victim_sex",
    "被害者の年齢": "victim_age",
    "被害者の職業": "victim_job",
    "現金被害の有無": "is_financial_damage",
    "施錠関係": "sejyou",
    "現金以外の主な被害品": "other_damage",
    "盗難防止装置の有無": "is_device",
    "file_name": "file_name",
}

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


def clean_data(file):
    # データの前処理
    d = pd.read_csv(file, encoding="shift-jis")
    # 年度はファイル名から取得
    d["file_name"] = os.path.basename(file)
    # 全角半角処理
    d.columns = [zen_to_han(c, kana=False) for c in d.columns]
    # 余分な文字列の置換
    d.columns = [re.sub('\(発生地\)|\(始期\)|\\n|"|\[|\]', "", c).strip() for c in d.columns]
    # 値の全角半角処理
    zen_han_cols = ["都道府県", "市区町村", "町丁目"]
    for c in zen_han_cols:
        d[c] = [zen_to_han(v, kana=False) for v in d[c].fillna("").astype(str)]
    # 列名を英字に変換
    d.columns = [cols_dict.get(c) for c in d.columns]
    d = add_all_cols(d, list(cols_dict.values()))
    # 全てを文字列に
    d = d.astype(str)
    return d


def main():
    # GCSにアップロード
    gcs_client = Gcs_client()
    gcs_client.create_bucket(BUCKET_NAME)
    files = glob.glob("../data/tokyo/*.csv", recursive=True)
    fields = []
    for c in cols_dict.values():
        fields.append(pa.field(c, pa.string()))
    table_schema = pa.schema(fields)
    all_objects = gcs_client.list_all_objects(BUCKET_NAME)
    for f in files:
        # データのクレンジング
        d = clean_data(f)
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
        gcs_client.upload_gcs(BUCKET_NAME, local_path, upload_path)

    # テーブル作成
    bq_client = Bigquery_cliant()
    dataset_name = "crime_dataset"
    table_name = "crime"
    bq_client.create_dataset(dataset_name)
    schema = bq_client.create_string_schema(cols_dict.values())
    table_id = f"{bq_client.client.project}.{dataset_name}.{table_name}"
    bq_client.create_external_table(BUCKET_NAME, table_id, schema, partitioned=True)


if __name__ == "__main__":
    # スクリプトの絶対パスを取得
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # カレントディレクトリをスクリプトのディレクトリに変更
    os.chdir(script_dir)
    main()
