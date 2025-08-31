import os
import io
import time
import json
import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st
from datetime import datetime, timedelta

# ========== 基本設定 ==========
st.set_page_config(page_title="台股研究網站", layout="wide")
st.title("威廷的股票網站")

DATA_XLSX = "sector_dashboard.xlsx"  # 可由排程自動更新
GROUPS_CSV = "groups.csv"            # 定義族群與個股
SHEET_NAME_FALLBACK = "連接器"        # 舊版相容：若只有單一工作表

# ========== Helpers ==========
@st.cache_data(show_spinner=False, ttl=24*3600)
def load_groups(groups_csv: str) -> pd.DataFrame:
    df = pd.read_csv(groups_csv)
    # 正規化欄位
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"ticker", "name", "sector"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"groups.csv 欄位缺少: {missing}")
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["sector"] = df["sector"].astype(str).str.strip()
    df["name"]   = df["name"].astype(str).str.strip()
    return df

@st.cache_data(show_spinner=False, ttl=24*3600)
def load_excel_dashboard(xlsx_path: str) -> dict:
    if not os.path.exists(xlsx_path):
        return {}
    xls = pd.ExcelFile(xlsx_path)
    sheets = {}
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        # 期待欄位：ticker,name,date,revenue (單位: 千元/萬元請自行統一)
        if {"ticker","name","date","revenue"}.issubset(set(df.columns)):
            df["date"] = pd.to_datetime(df["date"]).dt.date
            sheets[sheet] = df
    return sheets

@st.cache_data(show_spinner=True, ttl=24*3600)
def fetch_monthly_revenue_finmind(ticker: str, years: int = 3) -> pd.DataFrame:
    from FinMind.data import DataLoader
    token = os.environ.get("FINMIND_TOKEN")
    dl = DataLoader()
    if token:
        dl.login_by_token(api_token=token)

    start = datetime(2023, 1, 1).date()
    raw = dl.taiwan_stock_month_revenue(stock_id=str(ticker), start_date=start.isoformat())
    if raw.empty:
        return pd.DataFrame(columns=["ticker","name","date","revenue"])

    out = raw.rename(columns={
        "stock_id":   "ticker",
        "Revenue":    "revenue",
        "revenue":    "revenue",
        "date":       "date",
        "stock_name": "name",
    })
    out["ticker"] = out["ticker"].astype(str)
    out["date"]   = pd.to_datetime(out["date"]).dt.date

    # ① 這裡是重點：API 沒帶公司名時，用 groups.csv 裡的 name 來補
    name_map = groups_df.set_index("ticker")["name"].astype(str).to_dict()
    if "name" not in out.columns or out["name"].isna().all():
        out["name"] = out["ticker"].map(name_map)

    # ② 保證欄位齊全
    need = ["ticker","name","date","revenue"]
    for c in need:
        if c not in out.columns:
            out[c] = np.nan

    out = out[need].sort_values("date").reset_index(drop=True)
    return out



# 以 Excel → 優先；若無該族群資料 → FinMind 即時抓
@st.cache_data(show_spinner=False, ttl=12*3600)
def get_sector_data(sector: str, groups: pd.DataFrame, excel_sheets: dict) -> pd.DataFrame:
    tickers = groups.query("sector == @sector")["ticker"].unique().tolist()
    frames = []
    # 1) Excel 匹配該工作表
    if sector in excel_sheets:
        frames.append(excel_sheets[sector].query("ticker in @tickers"))
    # 2) 舊版相容：若 excel 只有單一工作表（如『連接器』），也嘗試過濾
    elif SHEET_NAME_FALLBACK in excel_sheets:
        frames.append(excel_sheets[SHEET_NAME_FALLBACK].query("ticker in @tickers"))

    # 3) 不足的股票 → FinMind 即時補
    have = set(pd.concat(frames)["ticker"]) if frames else set()
    need = [t for t in tickers if t not in have]
    for t in need:
        frames.append(fetch_monthly_revenue_finmind(t))

    if not frames:
        return pd.DataFrame(columns=["ticker","name","date","revenue"]) 

    df = pd.concat(frames, ignore_index=True).dropna(subset=["date"]).sort_values(["ticker","date"]) 
    return df

# KPI 計算
@st.cache_data(show_spinner=False, ttl=12*3600)
def enrich_kpi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]) 
    df = df.sort_values(["ticker","date"]).reset_index(drop=True)
    # 計算 MoM / YoY
    df["revenue_mom"] = df.groupby("ticker")["revenue"].pct_change()
    df["revenue_yoy"] = df.groupby("ticker")["revenue"].pct_change(12)
    return df

# ========== 資料載入 ==========
try:
    groups_df = load_groups(GROUPS_CSV)
except Exception as e:
    st.error(f"讀取 groups.csv 失敗：{e}")
    st.stop()

excel_sheets = load_excel_dashboard(DATA_XLSX)
all_sectors = sorted(groups_df["sector"].unique())

# ========== 側邊欄控制 ==========
st.sidebar.header("控制台 / Controls")
sector = st.sidebar.selectbox("選擇族群 (Sector)", options=all_sectors, index=0)

# 讀取該族群資料
sector_df = get_sector_data(sector, groups_df, excel_sheets)
sector_df = enrich_kpi(sector_df)

# ========== 版面 ==========
tab1, tab2 = st.tabs(["族群總覽 / Sector Overview", "個股鑽取 / Stock Drilldown"]) 

with tab1:
    st.subheader(f"📚 族群總覽：{sector}")
    if sector_df.empty:
        st.info("此族群目前沒有資料。請確認 groups.csv 與 Excel/FinMind。")
    else:
        latest = sector_df.sort_values("date").groupby("ticker").tail(1).copy()

        # 用 groups.csv 準備 ticker→name 對照
        name_map = groups_df.set_index("ticker")["name"]

        # 只有在沒有 name 欄位，才進行 merge（避免產生 name_x/name_y）
        if "name" not in latest.columns or latest["name"].isna().all():
            latest = latest.merge(groups_df[["ticker","name"]], on="ticker", how="left")
        else:
            # 已有 name 欄位就用 map 補空值，避免 KeyError
            latest["name"] = latest["name"].fillna(latest["ticker"].map(name_map))
        # 保證要顯示的欄位一定存在（缺的會自動補 NaN，而不會 KeyError）
        show_cols = ["ticker","name","date","revenue","revenue_mom","revenue_yoy"]
        latest = latest.reindex(columns=show_cols)

        st.dataframe(
            latest.rename(columns={
                "date": "最新月份",
                "revenue": "當月營收",
                "revenue_mom": "MoM",
                "revenue_yoy": "YoY",
            }),
            use_container_width=True
        )
        # 各股營收走勢 (疊圖)
        fig = px.line(
            sector_df,
            x="date", y="revenue", color="ticker",
            hover_data=["name"],
            title=f"{sector} 族群：月營收走勢"
        )
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("🔎 個股鑽取 / Stock-level Insights")
    if sector_df.empty:
        st.info("此族群目前沒有資料。")
    else:
        tickers_in_sector = (
            groups_df.query("sector == @sector")["ticker"].unique().tolist()
        )
        ticker_name_map = {
        t: f"{t} {groups_df.set_index('ticker').loc[t, 'name']}"
        for t in tickers_in_sector
        }
        ticker = st.selectbox(
            "選個股 (Ticker)",
            options=list(ticker_name_map.keys()),
            format_func=lambda x: ticker_name_map[x]
        )
        name = groups_df.set_index("ticker").loc[ticker, "name"]
        stock_df = sector_df.query("ticker == @ticker")

        col1, col2 = st.columns([2,1])
        with col1:
            st.markdown(f"### {ticker} {name}｜月營收 & YoY/MoM")
            fig1 = px.line(stock_df, x="date", y="revenue", title=f"{ticker} {name} 月營收")
            st.plotly_chart(fig1, use_container_width=True)

            # YoY / MoM 併圖（雙軸不做，避免誤讀；改用兩張圖）
            fig2 = px.line(stock_df, x="date", y="revenue_yoy", title="YoY (年增率)")
            st.plotly_chart(fig2, use_container_width=True)
            fig3 = px.line(stock_df, x="date", y="revenue_mom", title="MoM (月增率)")
            st.plotly_chart(fig3, use_container_width=True)

        with col2:
            # 最近 12 個月統計
            last12 = stock_df.tail(12)
            if not last12.empty:
                yoy = last12["revenue_yoy"].iloc[-1]
                mom = last12["revenue_mom"].iloc[-1]
                st.metric("最新 YoY", f"{yoy:.1%}" if pd.notna(yoy) else "—")
                st.metric("最新 MoM", f"{mom:.1%}" if pd.notna(mom) else "—")

            # 原始資料表
            st.markdown("#### 原始資料 / Raw Data")
            st.dataframe(stock_df[["date","revenue","revenue_yoy","revenue_mom"]].rename(columns={
                "date":"月份",
                "revenue":"營收",
            }), use_container_width=True)

st.caption("資料來源：FinMind；若提供 sector_dashboard.xlsx，則優先讀取檔案。")

