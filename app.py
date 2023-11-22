from datetime import date
import dateutil.relativedelta
import numpy as np
import streamlit as st
import folium
from folium.plugins import HeatMap
import plotly.express as px
import streamlit as st
from streamlit_folium import folium_static
from script.modules.gcp_class import Bigquery_cliant

bq = Bigquery_cliant()
table_id = st.secrets["bigquery"]["table_id"]
# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="オープンデータ", page_icon=":taxi:")


# 可視化用データの読み込み
@st.cache_resource
def load_data():
    bq = Bigquery_cliant()
    df = bq.read_sql(f"select * from  {table_id}")
    return df


st.title("東京犯罪ダッシュボード")
df_raw = load_data()
with st.sidebar:
    st.subheader("データフィルター")
    teguchi_list = ["ひったくり", "オートバイ盗", "自動車盗", "部品ねらい", "自動販売機ねらい", "車上ねらい", "自転車盗"]
    teguchi = st.multiselect("手口", teguchi_list)
    # 住所を選択
    ku_list = df_raw["city"].unique()
    city = st.multiselect("区", ku_list)

df = df_raw.copy()
# 仮に手口が選択されている場合は、フィルターする
if len(teguchi) != 0:
    condition1 = df["teguchi"].isin(teguchi)
    df = df[condition1]
# 仮に区が選択されている場合は、フィルターする
if len(city) != 0:
    condition2 = df["city"].isin(city)
    df = df[condition2]

tab1, tab2, tab3 = st.tabs(["全体", "詳細", "説明"])
with tab1:
    row0_1, row0_2 = st.columns([1, 4])
    with row0_1:
        # 今日から一年前の日付を取得
        today = date.today()
        past1y_date = today - dateutil.relativedelta.relativedelta(years=1)
        past2y_date = today - dateutil.relativedelta.relativedelta(years=2)
        cnt_crimes_past1y = df[df["occurrence_date"] == past1y_date][
            "tokyo_crimes_id"
        ].count()
        cnt_crimes_past2y = df[df["occurrence_date"] == past2y_date][
            "tokyo_crimes_id"
        ].count()
        delta_crimes = cnt_crimes_past1y - cnt_crimes_past2y
        st.subheader(f"{today}")
        st.metric(
            label="一年前の今日の犯罪件数",
            value=f"{int(cnt_crimes_past1y)}件",
            delta=f"{int(delta_crimes)}件 2年前と比べて",
            delta_color="inverse",
        )
    with row0_2:
        st.subheader("時系列推移")

        vis = (
            df.groupby(["occurrence_month", "teguchi"])["tokyo_crimes_id"]
            .count()
            .rename("犯罪件数")
            .reset_index()
        )
        # long_df = px.data.medals_long()
        fig = px.bar(
            vis,
            x="occurrence_month",
            y="犯罪件数",
            color="teguchi",
            # title="窃盗犯罪件数",
            color_discrete_sequence=px.colors.qualitative.Set1,
        )
        # X軸の日付形式を変更
        fig.update_xaxes(tickformat="%Y年%m月", title="発生年月")  # 例: 2022-11 の形式
        st.plotly_chart(fig, use_container_width=True)

    row1_1, row1_2 = st.columns(2)

    with row1_1:
        st.subheader("区ごとの犯罪発生数")
        # 区ごとの件数
        vis = (
            df.groupby("city")["tokyo_crimes_id"]
            .count()
            .rename("犯罪件数")
            .reset_index()
            .sort_values("犯罪件数")
        )
        # long_df = px.data.medals_long()
        fig = px.bar(
            vis,
            y="city",
            x="犯罪件数",
            orientation="h",  # 横向きの棒グラフに設定
            color_discrete_sequence=["blue"],  # 単色（青）に設定
        )
        # Y軸の目盛りを調整してすべて表示
        fig.update_yaxes(title="", automargin=True, dtick=1)  # 1つごとのデータポイントで目盛りを表示

        fig.update_layout(
            height=700,
            margin=dict(l=20, r=20, t=0, b=0),  # 左右上下の余白を設定
        )
        st.plotly_chart(fig, use_container_width=True)

    with row1_2:
        st.subheader("犯罪発生数ヒートマップ")
        vis = (
            df.groupby(["longitude", "latitude"])["tokyo_crimes_id"]
            .count()
            .rename("犯罪件数")
            .reset_index()
        )
        folium_map = folium.Map(location=[35.74, 139.70], zoom_start=10)
        HeatMap(
            data=vis[["longitude", "latitude", "犯罪件数"]].values.tolist(),
            radius=15,
        ).add_to(folium_map)
        folium_static(folium_map)
# 詳細タブ
with tab2:
    st.subheader("被害者属性")
    tf = ~df["victim_sex"].isin(["不明", "法人・団体"])
    vis = (
        df[tf]
        .groupby(["victim_age", "victim_sex"])["tokyo_crimes_id"]
        .count()
        .rename("犯罪件数")
        .reset_index()
    )
    fig = px.bar(
        vis,
        x="victim_sex",
        y="犯罪件数",
        color="victim_age",
        color_discrete_sequence=px.colors.sequential.Blues,
        category_orders={
            "victim_age": [
                "10歳未満",
                "10歳代",
                "20歳代",
                "30歳代",
                "40歳代",
                "50歳代",
                "60-64歳",
                "65-69歳",
                "70歳以上",
                "法人・団体、被害者なし",
            ]
        },
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=0, b=0),  # 左右上下の余白を設定,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.text("※属性データが記入されているデータのみを集計")
    row2_1, row2_2 = st.columns([3, 2])
    with row2_1:
        st.subheader("曜日・時間帯別犯罪件数")
        vis = (
            df.groupby(["day_of_week", "occurrence_time_str"])["tokyo_crimes_id"]
            .count()
            .rename("犯罪件数")
            .reset_index()
        )
        # ヒートマップの作成
        fig = px.density_heatmap(
            vis,
            x="occurrence_time_str",
            y="day_of_week",
            z="犯罪件数",  # カウント数で色の濃淡を表現
            color_continuous_scale="Viridis",  # 色のスケールを指定
            category_orders={
                "day_of_week": ["月", "火", "水", "木", "金", "土", "日"],
            },
        )
        fig.update_yaxes(title=None)
        fig.update_xaxes(title=None)
        fig.update_layout(
            margin=dict(l=20, r=20, t=0, b=0), coloraxis_colorbar_title_text=None
        )
        st.plotly_chart(fig, use_container_width=True)

    with row2_2:
        st.subheader("気温・降水量と犯罪平均件数")
        st.text("1日あたりの平均件数を表示しています")
        df_round = (
            df.groupby(
                ["precipitation_level", "average_temperature_level", "occurrence_date"]
            )["tokyo_crimes_id"]
            .count()
            .rename("犯罪件数")
            .reset_index()
        )
        vis = (
            df_round.groupby(["precipitation_level", "average_temperature_level"])[
                "犯罪件数"
            ]
            .mean()
            .reset_index()
        )

        # ヒートマップの作成
        fig = px.density_heatmap(
            vis,
            x="precipitation_level",
            y="average_temperature_level",
            z="犯罪件数",  # カウント数で色の濃淡を表現
            color_continuous_scale="Viridis",  # 色のスケールを指定
            # title="曜日別・時間帯別窃盗犯罪件数",
            category_orders={
                "average_temperature_level": ["10度以下", "10-19度", "20-24度", "25度以上"],
            },
        )
        # y軸のタイトルを変更
        fig.update_yaxes(title=None)
        fig.update_xaxes(title=None)
        fig.update_layout(
            margin=dict(l=20, r=20, t=0, b=0), coloraxis_colorbar_title_text=None
        )
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.markdown(
        """
        ### 免責事項
        - 本ダッシュボードに表示される情報は、[東京都警察](https://www.keishicho.metro.tokyo.lg.jp/about_mpd/jokyo_tokei/jokyo/hanzaihasseijyouhou.html)が提供する公開データに基づいています。ダッシュボードの作成者は、この情報の正確性、完全性、信頼性、適合性、または特定の目的への適用性に関しての保証を一切行いません。また、本ダッシュボードの使用によって生じるいかなる損害に対しても、責任を負いません。
        - ダッシュボードの利用者は、疑義が生じた場合には、情報の出典となる東京都警察のウェブサイトを直接参照し、一次情報を確認することが推奨されます。本ダッシュボードに掲載されている情報は参考情報であり、最終的な意思決定や行動を行う前に、利用者自身で情報の正確性を確認する責任があります。
        - ダッシュボードの運営者は、予告なくサービスを変更、または終了する権利を保持します。この変更または終了によって生じるいかなる影響についても、責任を負いません。

        ### 利用規約
        - データの使用: 本ダッシュボードに掲載されている情報は、東京都警察が提供するオープンデータを基にしています。利用の制限は元データに基づいています。
        - 規約の同意: 本ダッシュボードの使用を開始することにより、利用者は上記の免責事項および利用規約に同意したものとみなされます。

        ### ソースコードについて
        本アプリケーションのソースコードは[github](https://github.com/koy0208/crime-visualizer)上で公開しております
        """
    )
