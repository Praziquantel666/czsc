# crypto_connector.py
import os
import time
import pandas as pd
import ccxt  # 主要变更：引入CCXT库
from datetime import datetime, timedelta
from typing import List, Optional
from czsc import RawBar, Freq
from loguru import logger


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
    cache_path = os.environ.get("czsc_research_cache", r"D:\Quant_DATE\CZSC投研数据")
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