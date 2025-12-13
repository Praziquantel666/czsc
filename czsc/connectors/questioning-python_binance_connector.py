# -*- coding: utf-8 -*-
"""
使用 python-binance 作为 Binance 专用的简易连接器。

提供接口：
- `get_symbols()` 返回 DataFrame，包含 `symbol` 列
- `get_raw_bars(symbol, period, sdt, edt, **kwargs)` 返回 DataFrame，列为 `dt, open, high, low, close, vol, amount, symbol`

说明：需要安装 `python-binance`（`pip install python-binance`）。
API key/secret 可通过环境变量 `BINANCE_API_KEY` / `BINANCE_API_SECRET` 提供（可选，仅公共行情不需要）。
"""
import os
import time
import pandas as pd


def _import_binance():
    try:
        from binance.client import Client

        return Client
    except Exception as e:
        raise ImportError(
            "未能导入 python-binance，请先通过 `pip install python-binance` 安装：%s" % e
        )


def _get_client():
    Client = _import_binance()
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if api_key and api_secret:
        return Client(api_key, api_secret)
    return Client()


def get_symbols(**kwargs):
    client = _get_client()
    info = client.get_exchange_info()
    rows = []
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        rows.append({"symbol": s["symbol"], "base": s["baseAsset"], "quote": s["quoteAsset"]})
    return pd.DataFrame(rows)


def get_raw_bars(symbol="BTCUSDT", period="1m", sdt="20170101", edt=None, **kwargs):
    """使用 `Client.get_klines` 分页获取 K 线数据并返回标准 DataFrame。

    period: Binance 的 interval 字符串，如 '1m','3m','5m','15m','1h','4h','1d'
    sdt/edt: 可被 pandas 解析的时间字符串
    """
    client = _get_client()
    sdt = pd.to_datetime(sdt)
    edt = pd.to_datetime(edt) if edt is not None else pd.Timestamp.now()

    start_ms = int(sdt.timestamp() * 1000)
    end_ms = int(edt.timestamp() * 1000)

    all_rows = []
    cur_start = start_ms
    while cur_start < end_ms:
        klines = client.get_klines(symbol=symbol, interval=period, startTime=cur_start, endTime=end_ms, limit=1000)
        if not klines:
            break
        for k in klines:
            # k: [open_time, open, high, low, close, volume, close_time, ...]
            all_rows.append(
                {
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "vol": float(k[5]),
                    "close_time": int(k[6]),
                }
            )

        # 下一次起点为最后一根 K 的 open_time + 1 ms
        cur_start = int(klines[-1][0]) + 1
        time.sleep(0.2)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    # close_time 是该 K 线的结束时间（ms），转为东八区时间以保持与库中其它数据一致
    df["dt"] = pd.to_datetime(df["close_time"], unit="ms") + pd.Timedelta(hours=8)
    df["amount"] = df["vol"] * df["close"]
    df = df[["dt", "open", "high", "low", "close", "vol", "amount"]]
    df = df.drop_duplicates("dt", keep="last").sort_values("dt").reset_index(drop=True)
    df["symbol"] = symbol
    # 过滤 sdt/edt
    df = df[(df["dt"] >= sdt) & (df["dt"] <= edt)].reset_index(drop=True)
    return df


if __name__ == "__main__":
    # 简单示例
    print(get_symbols().head())
    print(get_raw_bars("BTCUSDT", "1m", "2024-01-01", "2024-01-02").head())
