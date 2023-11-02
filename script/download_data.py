import requests
import os
from bs4 import BeautifulSoup
import numpy as np


def download_files(url, file_dir, save_name=False):
    os.makedirs(file_dir, exist_ok=True)
    # Web上のファイルデータをダウンロード
    response = requests.get(url)

    # HTTP Responseのエラーチェック
    try:
        response_status = response.raise_for_status()
    except Exception as exc:
        print("Error:{}".format(exc))

    # HTTP Responseが正常な場合は下記実行
    if response_status == None:
        # open()関数にwbを渡し、バイナリ書き込みモードで新規ファイル生成
        if save_name:
            file = open(os.path.join(file_dir, save_name), "wb")
        else:
            file = open(os.path.join(file_dir, os.path.basename(url)), "wb")

        # 各チャンクをwrite()関数でローカルファイルに書き込む
        for chunk in response.iter_content(100000):
            file.write(chunk)

        # ファイルを閉じる
        file.close()


def run():
    os.makedirs("./data/shimane")
    # 　東京
    url = "https://www.keishicho.metro.tokyo.lg.jp/about_mpd/jokyo_tokei/jokyo/hanzaihasseijyouhou.html"
    file_dir = "./data/tokyo/"
    base_url = "https://www.keishicho.metro.tokyo.lg.jp/about_mpd/jokyo_tokei/jokyo/"
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")
    a_ref = soup.find_all("a")
    csv_urls = [c.get("href") for c in a_ref if ".csv" in c.get("href", default="None")]

    for csv_url in csv_urls:
        download_url = base_url + csv_url.replace("../../", "").replace("./", "")
        download_files(download_url, file_dir)


if __name__ == "__main__":
    run()
