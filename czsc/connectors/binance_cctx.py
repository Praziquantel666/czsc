# crypto_connector.py
import os
import time
import pandas as pd
import ccxt  # 主要变更：引入CCXT库
from datetime import datetime, timedelta
from typing import List, Optional
from czsc import RawBar, Freq
from loguru import logger
from pathlib import Path
import czsc


class BinanceFuturesAPI:
    """使用CCXT库的币安合约API封装"""

    def __init__(self, api_key: str = None, api_secret: str = None, proxy: str = 'http://127.0.0.1:7897'):
        # 主要变更：使用CCXT初始化币安合约交易所实例
        self.exchange = ccxt.binance({
            'apiKey': api_key or os.getenv("BINANCE_API_KEY"),
            'secret': api_secret or os.getenv("BINANCE_API_SECRET"),
            'enableRateLimit': True,  # 启用CCXT内置的速率限制保护[1](@ref)
            'options': {
                'defaultType': 'future'  # 设置为合约交易[1](@ref)
            },
        })
        # 设置代理
        if proxy:
            self.exchange.proxies = {
                'http': proxy,
                'https': proxy,
            }
        # 可选：设置其他CCXT参数，如超时时间
        self.exchange.timeout = 30000

    def get_klines(self, symbol: str, interval: str,
                   start_time: int = None, end_time: int = None, limit: int = 1500) -> List[list]:
        """使用CCXT获取K线数据 (OHLCV)"""
        # 将时间戳转换为CCXT所需的毫秒单位
        since = start_time
        params = {}
        if end_time:
            params['until'] = end_time  # 某些交易所支持until参数

        try:
            # 使用CCXT的fetch_ohlcv方法[1,3](@ref)
            ohlcv = self.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=interval,
                since=since,
                limit=limit,
                params=params
            )
            return ohlcv
        except ccxt.NetworkError as e:
            logger.error(f"网络错误: {e}")
            raise
        except ccxt.ExchangeError as e:
            logger.error(f"交易所错误: {e}")
            raise

    def get_exchange_info(self) -> dict:
        """使用CCXT获取交易对信息"""
        try:
            markets = self.exchange.load_markets()
            return markets
        except Exception as e:
            logger.error(f"获取交易对信息失败: {e}")
            raise

    def get_server_time(self) -> dict:
        """使用CCXT获取服务器时间"""
        try:
            time = self.exchange.fetch_time()
            return {'serverTime': time}
        except Exception as e:
            logger.error(f"获取服务器时间失败: {e}")
            raise


def format_binance_kline(symbol: str, kline_data: list, freq: Freq) -> List[RawBar]:
    """格式化CCXT返回的K线为RawBar对象"""
    bars = []
    for i, k in enumerate(kline_data):
        # CCXT的OHLCV数据格式: [timestamp, open, high, low, close, volume]
        bar = RawBar(
            symbol=symbol,
            dt=datetime.fromtimestamp(k[0] / 1000),  # CCXT时间戳也是毫秒
            id=i,
            freq=freq,
            open=float(k[1]),
            high=float(k[2]),
            low=float(k[3]),
            close=float(k[4]),
            vol=float(k[5]),  # 成交量
            amount=float(k[5]) * float(k[4])  # 估算成交额(成交量 * 收盘价)
        )
        bars.append(bar)
    return bars


def get_crypto_bars(symbol: str, freq: Freq, sdt: datetime, edt: datetime,
                    api: BinanceFuturesAPI = None, **kwargs) -> List[RawBar]:
    """获取加密货币K线数据"""
    # 频率映射
    freq_map = {
        Freq.F1: "1m",
        Freq.F5: "5m",
        Freq.F15: "15m",
        Freq.F30: "30m",
        Freq.F60: "1h",
        Freq.D: "1d"
    }
    interval = freq_map.get(freq)

    if not api:
        api = BinanceFuturesAPI(proxy=kwargs.get("proxy", 'http://127.0.0.1:7897'))

    # 时间转换 (CCXT需要毫秒时间戳)
    start_ts = int(sdt.timestamp() * 1000)
    end_ts = int(edt.timestamp() * 1000)

    # 获取K线数据
    klines = api.get_klines(symbol, interval, start_time=start_ts, end_time=end_ts)
    return format_binance_kline(symbol, klines, freq)


def update_crypto_cache(symbol: str, freq: Freq, **kwargs):
    """更新加密货币缓存数据"""
    cache_path = os.environ.get("czsc_research_cache", r"Q:\Quant_DATE\CZSC投研数据")
    if not os.path.exists(cache_path):
        raise ValueError(
            f"请设置环境变量 czsc_research_cache 为投研共享数据的本地缓存路径，当前路径不存在:{cache_path}"
        )

    crypto_path = os.path.join(cache_path, "加密货币")
    os.makedirs(crypto_path, exist_ok=True)
    file_path = os.path.join(crypto_path, f"{symbol}_{freq.value}.parquet")

    # 获取最新数据
    edt = datetime.now()
    if os.path.exists(file_path):
        df_old = pd.read_parquet(file_path)
        last_dt = df_old["dt"].max().to_pydatetime()
        sdt = last_dt + timedelta(seconds=1)  # 避免重复
    else:
        sdt = kwargs.get('sdt', edt - timedelta(days=30))  # 默认获取30天数据

    kwargs.pop('sdt', None)
    bars = get_crypto_bars(symbol, freq, sdt, edt, **kwargs)
    if not bars:
        logger.info(f"无新数据：{symbol} {freq.value}")
        return

    # 转换为DataFrame并保存
    data = []
    for bar in bars:
        item = bar.__dict__.copy()
        # 将freq枚举转换为字符串
        item['freq'] = item['freq'].value
        data.append(item)

    df_new = pd.DataFrame(data)
    if os.path.exists(file_path):
        df_old = pd.read_parquet(file_path)
        df = pd.concat([df_old, df_new]).drop_duplicates(subset=["dt"])
    else:
        df = df_new

    # 关键修改：删除无效的cache列
    if 'cache' in df.columns:
        df = df.drop(columns=['cache'])

    df.to_parquet(file_path)
    logger.info(f"已更新 {symbol} {freq.value} 数据，最新K线时间：{df['dt'].max()}")


def get_crypto_symbols(api: BinanceFuturesAPI = None) -> List[str]:
    """获取所有加密货币交易对"""
    if not api:
        api = BinanceFuturesAPI()
    try:
        info = api.get_exchange_info()
        # CCXT返回的市场信息格式不同，需要适配
        symbols = [symbol for symbol, market in info.items()
                  if market.get('active', False) and market.get('future', False)]
        return symbols
    except Exception as e:
        logger.error(f"获取交易对失败: {e}")
        return []


def get_symbols(exchange: str = "币安期货", **kwargs) -> pd.DataFrame:
    """兼容 ccxt_connector.get_symbols 的接口，返回 DataFrame，包含 `symbol`, `base`, `quote`, `price_size` 列。"""
    # 使用 BinanceFuturesAPI 的 load_markets 结果
    api = BinanceFuturesAPI()
    markets = api.get_exchange_info()
    rows = []
    for symbol, market in markets.items():
        try:
            base = market.get("base")
            quote = market.get("quote")
            precision = market.get("precision", {})
            price_size = precision.get("price") if isinstance(precision, dict) else None
            rows.append({"ccxt_symbol": symbol, "base": base, "quote": quote, "price_size": price_size})
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if not df.empty:
        df["symbol"] = df["base"].astype(str) + df["quote"].astype(str)
    return df


def __api_fetch_ohlcv(api: BinanceFuturesAPI, symbol: str, sdt, edt, interval: str):
    """使用 BinanceFuturesAPI 分页获取 OHLCV 并返回 DataFrame（兼容 ccxt_connector 行为）。"""
    since = int(pd.to_datetime(sdt).timestamp() * 1000)
    until = int(pd.to_datetime(edt).timestamp() * 1000)

    all_klines = []
    cur = since
    while cur < until:
        try:
            klines = api.get_klines(symbol=symbol, interval=interval, start_time=cur, end_time=until, limit=1000)
        except Exception as e:
            logger.warning(f"请求 klines 失败 {symbol} {interval} {cur}: {e}")
            break

        if not klines:
            break

        all_klines.extend(klines)
        # klines 每项的第0位为 open timestamp（ms）
        last_ts = int(klines[-1][0])
        cur = last_ts + 1
        # sleep 避免触发限流
        time.sleep(0.2)

    if not all_klines:
        return pd.DataFrame()

    df = pd.DataFrame(all_klines)
    # klines 格式通常为 [open_ts, open, high, low, close, volume, close_ts,...]，有些实现没有 close_ts
    # 保守处理：如果第6列存在则使用为 close_time，否则使用 open_ts + interval
    if df.shape[1] >= 7:
        df = df.iloc[:, :7]
        df.columns = ["open_ts", "open", "high", "low", "close", "vol", "close_ts"]
        # 使用 close_ts 作为 K 线结束时间
        df["dt"] = pd.to_datetime(df["close_ts"], unit="ms")
    else:
        df = df.iloc[:, :6]
        df.columns = ["open_ts", "open", "high", "low", "close", "vol"]
        df["dt"] = pd.to_datetime(df["open_ts"], unit="ms")

    df["amount"] = df["vol"] * df["close"]

    # 根据 interval 把 dt 调整为 K 线结束时间（与 ccxt_connector 保持一致）
    if interval == "4h":
        df["dt"] = df["dt"] + pd.Timedelta(hours=4)
    elif interval == "2h":
        df["dt"] = df["dt"] + pd.Timedelta(hours=2)
    elif interval == "1h":
        df["dt"] = df["dt"] + pd.Timedelta(hours=1)
    elif interval == "30m":
        df["dt"] = df["dt"] + pd.Timedelta(minutes=30)
    elif interval == "15m":
        df["dt"] = df["dt"] + pd.Timedelta(minutes=15)
    elif interval == "5m":
        df["dt"] = df["dt"] + pd.Timedelta(minutes=5)
    elif interval == "1m":
        df["dt"] = df["dt"] + pd.Timedelta(minutes=1)

    # 时区转换：UTC -> Asia/Shanghai
    df["dt"] = pd.to_datetime(df["dt"]) + pd.Timedelta(hours=8)

    df = df.rename(columns={"open": "open", "high": "high", "low": "low", "close": "close", "vol": "vol", "amount": "amount"})
    df = df[["dt", "open", "high", "low", "close", "vol", "amount"]]
    df = df.drop_duplicates("dt", keep="last").sort_values("dt").reset_index(drop=True)
    return df


def get_raw_bars(symbol="BTCUSDT", period="4h", sdt="20240101", edt="20240308", **kwargs):
    """兼容 ccxt_connector.get_raw_bars 的接口。

    支持 period: 1m/5m/15m/30m/1h/2h/4h/6h/8h/12h/1d
    """
    logger_local = kwargs.get("logger", logger)
    api = kwargs.get("api") or BinanceFuturesAPI()

    timeframes = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h",
        "6h": "6h",
        "8h": "8h",
        "12h": "12h",
        "1d": "1d",
    }
    if period not in timeframes:
        raise ValueError(f"不支持的时间周期: {period}")

    df = __api_fetch_ohlcv(api, symbol, sdt, edt, timeframes[period])
    if df.empty:
        return df
    df["symbol"] = symbol
    df = df[(df["dt"] <= pd.to_datetime(edt)) & (df["dt"] >= pd.to_datetime(sdt))]
    # 过滤未完成的K线
    if not df.empty and df["dt"].max() > pd.Timestamp.now():
        df = df.iloc[:-1]
    logger_local.info(f"get_raw_bars::获取 {symbol} {period} K线数据，时间段：{sdt} - {edt}，K线数量：{len(df)}")
    return df


def get_latest_klines(symbol, period, sdt=None, **kwargs):
    """参考 ccxt_connector.get_latest_klines 的行为，实现本地缓存的增量获取。"""
    logger_local = kwargs.get("logger", logger)
    cache_path = Path(kwargs.get("cache_path", czsc.home_path))
    cache_path.mkdir(exist_ok=True, parents=True)

    sdt_pd = pd.to_datetime(sdt) if sdt else pd.to_datetime("20170101")
    edt = pd.Timestamp.now() + pd.Timedelta(days=2)
    file_cache = Path(f"{cache_path}/klines_{symbol}_{period}_{sdt_pd.strftime('%Y%m%d')}.feather")

    if file_cache.exists():
        df = pd.read_feather(file_cache)
        logger_local.info(f"读取缓存数据：{file_cache}，最新时间：{df['dt'].max()}")
        _sdt = df["dt"].max() - pd.Timedelta(days=1)
    else:
        df = pd.DataFrame()
        _sdt = sdt_pd

    df1 = get_raw_bars(symbol=symbol, period=period, sdt=_sdt, edt=edt)
    logger_local.info(f"获取 {symbol} {period} K线数据，时间段：{_sdt} - {edt}，K线数量：{len(df1)}")

    df2 = pd.concat([df, df1], ignore_index=True)
    df2 = df2.drop_duplicates("dt", keep="last")
    df2 = df2.sort_values("dt").reset_index(drop=True)
    df2.to_feather(file_cache)

    df3 = df2[df2["dt"] > sdt_pd].reset_index(drop=True)
    logger_local.info(f"获取 {symbol} {period} K线数据，时间段：{df3['dt'].min()} - {df3['dt'].max()}，K线数量：{len(df3)}")
    return df3