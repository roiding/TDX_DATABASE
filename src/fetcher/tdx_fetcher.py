"""
TDX 行情数据获取器

基于 pytdx 封装，提供：
- 多服务器自动 failover
- 请求重试
- 分页拉取所有历史K线
- 除权除息数据获取
- 股票列表获取
"""

import time
from datetime import datetime
from pytdx.hq import TdxHq_API
from src.config import get_config
from src.utils.logger import logger

# pytdx K线周期常量
FREQ_5MIN = 0
FREQ_1MIN = 8


class TdxFetcher:

    def __init__(self):
        cfg = get_config()
        self.servers = cfg.get("tdx_servers", [{"host": "119.147.212.81", "port": 7709}])
        sync_cfg = cfg.get("sync", {})
        self.batch_size = sync_cfg.get("batch_size", 800)
        self.max_pages = sync_cfg.get("max_pages", 20)
        self.request_interval = sync_cfg.get("request_interval", 0.1)
        self.max_retries = sync_cfg.get("max_retries", 3)

        self._api = TdxHq_API()
        self._connected = False
        self._current_server_idx = 0

    # ============================================================
    # 连接管理
    # ============================================================

    def connect(self) -> bool:
        """尝试连接，自动遍历服务器列表"""
        for i, server in enumerate(self.servers):
            try:
                result = self._api.connect(server["host"], server["port"])
                if result:
                    self._connected = True
                    self._current_server_idx = i
                    logger.info(f"已连接 TDX 服务器: {server['host']}:{server['port']}")
                    return True
            except Exception as e:
                logger.warning(f"连接 {server['host']}:{server['port']} 失败: {e}")
                continue

        logger.error("所有 TDX 服务器均连接失败")
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
        """带重试和自动重连的调用封装"""
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
    # 股票列表
    # ============================================================

    def fetch_stock_list(self) -> list[dict]:
        """
        获取沪深两市全部证券列表，过滤出A股。
        返回: [{'stock_code', 'market', 'stock_name', 'stock_type'}, ...]
        """
        all_stocks = []

        for market in [0, 1]:  # 0=深圳, 1=上海
            start = 0
            while True:
                data = self._retry_on_failure(self._api.get_security_list, market, start)
                if not data:
                    break
                for item in data:
                    code = item["code"]
                    stock_type = self._classify_stock(code, market)
                    if stock_type is not None:
                        all_stocks.append({
                            "stock_code": code,
                            "market": market,
                            "stock_name": item.get("name", ""),
                            "stock_type": stock_type,
                        })
                start += len(data)

        logger.info(f"获取到 {len(all_stocks)} 只A股证券")
        return all_stocks

    @staticmethod
    def _classify_stock(code: str, market: int) -> int | None:
        """
        根据代码前缀分类证券类型。
        返回: 0=股票, 1=指数, 2=ETF, None=不纳入
        """
        if market == 0:  # 深圳
            if code.startswith(("000", "001", "002", "003", "300", "301")):
                return 0  # A股
            if code.startswith("399"):
                return 1  # 指数
            if code.startswith(("159",)):
                return 2  # ETF
        elif market == 1:  # 上海
            if code.startswith(("600", "601", "603", "605", "688", "689")):
                return 0  # A股
            if code.startswith(("000", "880")):
                return 1  # 指数
            if code.startswith(("510", "511", "512", "513", "515", "516", "518", "560", "561", "562", "563", "588")):
                return 2  # ETF
        return None  # 不纳入（B股、债券、权证等）

    # ============================================================
    # K线数据
    # ============================================================

    def fetch_kline(
        self,
        frequency: int,
        market: int,
        stock_code: str,
        max_pages: int = None,
        stop_before: datetime = None,
    ) -> list[dict]:
        """
        分页拉取K线数据（从最新往回翻页）。

        Args:
            frequency: FREQ_1MIN 或 FREQ_5MIN
            market: 0=深圳, 1=上海
            stock_code: 股票代码
            max_pages: 最大翻页次数，None则使用默认值
            stop_before: 遇到此时间之前的数据则停止（用于增量同步）

        Returns:
            list of dict, 按时间升序
        """
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

                # 增量模式：遇到已有数据则停止
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

        # 按时间升序排列
        all_bars.sort(key=lambda x: x["dt"])
        return all_bars

    @staticmethod
    def _parse_bar_datetime(bar: dict) -> datetime | None:
        """解析 pytdx 返回的K线时间"""
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

    def fetch_xdxr(self, market: int, stock_code: str) -> list[dict]:
        """
        获取除权除息事件数据。
        返回: [{'stock_code', 'market', 'ex_date', 'category', 'fenhong', ...}, ...]
        """
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
