import os
import pandas as pd
from datetime import datetime

from FinMind.data import DataLoader

def fetch_all(groups_csv: str, years: int = 3) -> dict:
    df = pd.read_csv(groups_csv)
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"ticker", "name", "sector"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"groups.csv 欄位缺少: {missing}。請把表頭改成 ticker,name,sector")

    # 建 ticker→name 對照，API 沒帶 stock_name 時補用
    name_map = df.assign(ticker=df["ticker"].astype(str)).set_index("ticker")["name"].astype(str).to_dict()
    sectors = sorted(df["sector"].unique())

    token = os.environ.get("FINMIND_TOKEN", None)
    dl = DataLoader()
    if token:
        dl.login_by_token(api_token=token)

    sheets: dict[str, pd.DataFrame] = {}
    for sector in sectors:
        frames = []
        for t in df.query("sector == @sector")["ticker"].astype(str).unique():
            sub = dl.taiwan_stock_month_revenue(
                stock_id=t,
                start_date=(datetime.today().date().replace(day=1).replace(year=datetime.today().year - years)).isoformat()
            )
            if sub.empty:
                continue

            # 欄位正規化
            sub = sub.rename(columns={
                "stock_id": "ticker",
                "date": "date",
                "Revenue": "revenue",
                "revenue": "revenue",
                "stock_name": "name",
            })

            # 補公司名（API 若沒帶）
            sub["ticker"] = sub["ticker"].astype(str)
            if "name" not in sub.columns or sub["name"].isna().all():
                sub["name"] = sub["ticker"].map(name_map)

            sub = sub[["ticker", "name", "date", "revenue"]]
            frames.append(sub)

        if frames:
            out = pd.concat(frames, ignore_index=True)
            out["date"] = pd.to_datetime(out["date"]).dt.date
            out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
            sheets[sector] = out

    return sheets


if __name__ == "__main__":
    sheets = fetch_all("groups.csv", years=3)
    if not sheets:
        raise SystemExit("No data fetched. Check groups.csv or FinMind token.")
    with pd.ExcelWriter("sector_dashboard.xlsx", engine="openpyxl") as writer:
        for sheet, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet[:31], index=False)  # Excel sheet 名稱限 31 字
    print("Updated sector_dashboard.xlsx ✅")

