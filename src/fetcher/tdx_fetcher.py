from __future__ import annotations

"""
TDX 数据获取器

- 股票列表: mootdx (stock_all，完整可靠)
- K线数据/除权除息: pytdx 协议直连行情服务器
- 服务器自动选优: 从 pytdx 内置列表中选延迟最低的
"""

import time
from datetime import datetime

from pytdx.hq import TdxHq_API

from src.config import get_config
from src.utils.logger import logger

FREQ_5MIN = 0
FREQ_1MIN = 8

# 模块级缓存：选优结果只做一次，进程内复用
_cached_best_server = None


def _select_best_server_once():
    """首次调用时从 pytdx 内置列表中选最快服务器，之后复用缓存"""
    global _cached_best_server
    if _cached_best_server is not None:
        return _cached_best_server

    from pytdx.config.hosts import hq_hosts
    api = TdxHq_API()
    logger.info(f"正在从 {len(hq_hosts)} 台服务器中自动选优（仅首次）...")
    best = None
    best_time = float("inf")

    for name, host, port in hq_hosts:
        try:
            start = time.time()
            r = api.connect(host, port, time_out=3)
            elapsed = time.time() - start
            if r:
                api.disconnect()
                if elapsed < best_time:
                    best = (host, port, name)
                    best_time = elapsed
                if elapsed < 0.05:
                    break
        except Exception:
            continue

    if best:
        logger.info(f"选优完成: {best[2]} ({best[0]}:{best[1]}) 延迟 {best_time*1000:.0f}ms")
        _cached_best_server = best
    else:
        logger.error("所有 TDX 服务器均连接失败")
    return best


class TdxFetcher:

    def __init__(self):
        cfg = get_config()
        sync_cfg = cfg.get("sync", {})
        self.batch_size = sync_cfg.get("batch_size", 800)
        self.max_pages = sync_cfg.get("max_pages", 50)
        self.request_interval = sync_cfg.get("request_interval", 0.1)
        self.max_retries = sync_cfg.get("max_retries", 3)

        self._api = TdxHq_API()
        self._connected = False

    # ============================================================
    # 连接管理
    # ============================================================

    def connect(self) -> bool:
        server = _select_best_server_once()
        if not server:
            return False

        host, port, name = server
        try:
            if self._api.connect(host, port, time_out=5):
                self._connected = True
                logger.info(f"已连接: {name} ({host}:{port})")
                return True
        except Exception:
            pass

        # 缓存的服务器失效，清除缓存重新选优
        global _cached_best_server
        _cached_best_server = None
        logger.warning("缓存服务器失效，重新选优")
        server = _select_best_server_once()
        if not server:
            return False

        host, port, name = server
        try:
            if self._api.connect(host, port, time_out=5):
                self._connected = True
                logger.info(f"已连接: {name} ({host}:{port})")
                return True
        except Exception as e:
            logger.error(f"连接失败: {e}")
        return False

    def disconnect(self):
        if self._connected:
            try:
                self._api.disconnect()
            except Exception:
                pass
            self._connected = False

    def _ensure_connected(self):
        if not self._connected:
            if not self.connect():
                raise ConnectionError("无法连接 TDX 服务器")

    def _retry_on_failure(self, func, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                self._ensure_connected()
                result = func(*args, **kwargs)
                if self.request_interval > 0:
                    time.sleep(self.request_interval)
                return result
            except Exception as e:
                logger.warning(f"请求失败 (第{attempt + 1}次): {e}")
                self._connected = False
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                    self.connect()
                else:
                    raise

    # ============================================================
    # 股票列表 (mootdx)
    # ============================================================

    def fetch_stock_list(self) -> list[dict]:
        """通过 mootdx 获取沪深全部证券列表，过滤出A股"""
        from mootdx.quotes import Quotes

        # 复用已选优的服务器
        server = _select_best_server_once()
        if server:
            host, port, _ = server
            srv = (host, port)
        else:
            srv = None

        client = Quotes.factory(market="std", server=srv, timeout=15)

        stocks = []
        for market in [0, 1]:
            label = "深圳" if market == 0 else "上海"
            df = client.stocks(market=market)
            logger.info(f"{label} 获取到 {len(df)} 条证券")

            for _, row in df.iterrows():
                code = str(row["code"])
                name = str(row.get("name", ""))
                stock_type = self._classify_stock(code, market)
                if stock_type is not None:
                    stocks.append({
                        "stock_code": code,
                        "market": market,
                        "stock_name": name,
                        "stock_type": stock_type,
                    })

        client.close()
        logger.info(f"过滤后 A股证券: {len(stocks)} 只")
        return stocks

    @staticmethod
    def _classify_stock(code: str, market: int) -> int | None:
        """0=股票, 1=指数, 2=ETF, None=不纳入"""
        if market == 0:  # 深圳
            if code.startswith(("000", "001", "002", "003", "300", "301")):
                return 0
            if code.startswith(("430", "830", "831", "832", "833", "834", "835",
                                "836", "837", "838", "839", "870", "871", "872",
                                "873", "920")):
                return 0  # 北交所
            if code.startswith("399"):
                return 1
            if code.startswith("159"):
                return 2
        elif market == 1:  # 上海
            if code.startswith(("600", "601", "603", "605", "688", "689")):
                return 0
            if code.startswith(("000", "880")):
                return 1
            if code.startswith(("510", "511", "512", "513", "515", "516", "518",
                                "560", "561", "562", "563", "588")):
                return 2
        return None

    # ============================================================
    # K线数据
    # ============================================================

    def fetch_kline(self, frequency, market, stock_code, max_pages=None, stop_before=None) -> list[dict]:
        if max_pages is None:
            max_pages = self.max_pages

        all_bars = []
        offset = 0
        should_stop = False

        for _ in range(max_pages):
            data = self._retry_on_failure(
                self._api.get_security_bars, frequency, market, stock_code, offset, self.batch_size
            )
            if not data:
                break

            for bar in data:
                dt = self._parse_bar_datetime(bar)
                if dt is None:
                    continue
                if stop_before and dt <= stop_before:
                    should_stop = True
                    continue

                all_bars.append({
                    "stock_code": stock_code,
                    "market": market,
                    "dt": dt,
                    "open": round(bar["open"], 3),
                    "high": round(bar["high"], 3),
                    "low": round(bar["low"], 3),
                    "close": round(bar["close"], 3),
                    "volume": int(bar.get("vol", 0)),
                    "amount": round(bar.get("amount", 0), 2),
                })

            if should_stop or len(data) < self.batch_size:
                break
            offset += self.batch_size

        all_bars.sort(key=lambda x: x["dt"])
        return all_bars

    @staticmethod
    def _parse_bar_datetime(bar: dict) -> datetime | None:
        try:
            dt_str = bar.get("datetime", "")
            if not dt_str:
                return None
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            return None

    # ============================================================
    # 除权除息
    # ============================================================

    def fetch_xdxr(self, market, stock_code) -> list[dict]:
        data = self._retry_on_failure(self._api.get_xdxr_info, market, stock_code)
        if not data:
            return []

        rows = []
        for item in data:
            try:
                ex_date = f"{item['year']:04d}-{item['month']:02d}-{item['day']:02d}"
                rows.append({
                    "stock_code": stock_code,
                    "market": market,
                    "ex_date": ex_date,
                    "category": item.get("category", 1),
                    "fenhong": item.get("fenhong", 0) or 0,
                    "peigujia": item.get("peigujia", 0) or 0,
                    "songzhuangu": item.get("songzhuangu", 0) or 0,
                    "peigu": item.get("peigu", 0) or 0,
                    "suogu": item.get("suogu", 0) or 0,
                    "panqianliutong": item.get("panqianliutong", 0) or 0,
                    "panhouliutong": item.get("panhouliutong", 0) or 0,
                    "qianzongguben": item.get("qianzongguben", 0) or 0,
                    "houzongguben": item.get("houzongguben", 0) or 0,
                })
            except (KeyError, TypeError):
                continue
        return rows

    # ============================================================
    # Context manager
    # ============================================================

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
