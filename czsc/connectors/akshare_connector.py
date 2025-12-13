# -*- coding: utf-8 -*-
"""
轻量的 akshare 数据源适配器，目标 API 与现有 `ccxt_connector` 保持类似：
- `get_symbols()` 返回 DataFrame，包含 `symbol` 列
- `get_raw_bars(symbol, period, sdt, edt, **kwargs)` 返回标准 DataFrame，包含 `dt, open, high, low, close, vol, amount, symbol`

说明：akshare 的函数名与参数在不同版本中可能有差异，代码会尝试几种常见的调用方法。
如果本机未安装 `akshare`，会抛出明确提示。
"""
import logging
from pathlib import Path
import pandas as pd
import datetime

logger = logging.getLogger("akshare_connector")


def _import_akshare():
    try:
        import akshare as ak

        return ak
    except Exception as e:
        raise ImportError(
            "未能导入 akshare，请先通过 `pip install akshare` 安装：%s" % e
        )


def get_symbols(**kwargs):
    """返回 A 股代码与名称（DataFrame，包含 `symbol` 列，symbol 格式为 6 位代码）"""
    ak = _import_akshare()
    # akshare 提供 stock_info_a_code_name 函数返回 code/name
    if hasattr(ak, "stock_info_a_code_name"):
        df = ak.stock_info_a_code_name()
        # 返回列可能是 code/name 或 code/name（中文），统一为 symbol/name
        if "code" in df.columns:
            df = df.rename(columns={"code": "symbol"})
        elif "代码" in df.columns:
            df = df.rename(columns={"代码": "symbol"})
        return df

    raise RuntimeError("当前 akshare 版本不支持获取 A 股代码列表，请升级 akshare")


def _normalize_df(df, symbol):
    # 将 akshare 各种返回格式规范成统一格式
    col_map = {}
    if "时间" in df.columns:
        col_map["时间"] = "dt"
    if "datetime" in df.columns:
        col_map["datetime"] = "dt"
    if "日期" in df.columns:
        col_map["日期"] = "dt"

    # 价格列中文->英文
    if "开盘" in df.columns:
        col_map.update({"开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "vol", "成交额": "amount"})
    if "open" in df.columns:
        col_map.update({k: k for k in ["open", "high", "low", "close", "vol", "amount"]})

    df = df.rename(columns=col_map)

    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"])
    else:
        # 若没有 dt 列，尝试从 index 或其他列构造
        if df.index.dtype.kind in "uif":
            try:
                df["dt"] = pd.to_datetime(df.index)
            except Exception:
                pass

    # 标准列
    for c in ["open", "high", "low", "close", "vol"]:
        if c not in df.columns:
            df[c] = None

    # amount 有时不存在
    if "amount" not in df.columns:
        try:
            df["amount"] = df["vol"] * df["close"]
        except Exception:
            df["amount"] = None

    df = df[[c for c in ["dt", "open", "high", "low", "close", "vol", "amount"] if c in df.columns]]
    df = df.drop_duplicates("dt", keep="last").sort_values("dt").reset_index(drop=True)
    df["symbol"] = symbol
    return df


def get_raw_bars(symbol, period="1m", sdt="20170101", edt=None, **kwargs):
    """获取指定 symbol 的分钟/日线数据，尽量返回与 `ccxt_connector.get_raw_bars` 相同格式。

    Args:
        symbol: 支持 6 位 A 股代码（如 600519），或带市场前缀（sh600519/sz000001）
        period: 支持 '1m','5m','15m','30m','60m','1d' 等
        sdt/edt: 支持字符串，可被 pd.to_datetime 解析
    """
    ak = _import_akshare()

    sdt = pd.to_datetime(sdt)
    edt = pd.to_datetime(edt) if edt is not None else pd.Timestamp.now()

    # akshare 的 minute 接口在不同版本函数名不同，这里尝试多个候选
    candidates = [
        ("stock_zh_a_minute", {"symbol": symbol, "period": period.rstrip("m")}),
        ("stock_zh_a_minute_em", {"symbol": symbol, "period": period}),
        ("stock_zh_a_hist_min_em", {"symbol": symbol, "period": period, "start_date": sdt.strftime("%Y-%m-%d"), "end_date": edt.strftime("%Y-%m-%d")}),
    ]

    df = None
    for name, params in candidates:
        if hasattr(ak, name):
            try:
                func = getattr(ak, name)
                df_try = func(**params)
                if df_try is None or len(df_try) == 0:
                    continue
                df = df_try
                logger.info(f"akshare: 使用 {name} 获得数据，rows={len(df)}")
                break
            except Exception as e:
                logger.debug(f"尝试 akshare.{name} 失败: {e}")

    if df is None:
        raise RuntimeError("未能通过 akshare 获取分钟数据，请检查 akshare 版本或函数名")

    df = _normalize_df(pd.DataFrame(df), symbol)
    # 过滤时间范围
    df = df[(df["dt"] >= sdt) & (df["dt"] <= edt)].reset_index(drop=True)
    return df


if __name__ == "__main__":
    # 简单自测用例（仅在有 akshare 环境时可用）
    try:
        print(get_symbols().head())
    except Exception as e:
        print("akshare not available:", e)
