"""
全量同步 — 首次铺底 / 恢复续跑

支持阶段：
- stock_list
- xdxr
- 5min
- 1min

设计目标：
1. 可按阶段单独运行，兼容线上已有进度
2. xdxr 若已在 sync_log 中成功完成过，则可跳过
3. 5min / 1min 基于单股记录数阈值判断是否需要继续抓取
4. K线全量同步支持多线程并发，加速铺底
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config import get_config
from src.fetcher.tdx_fetcher import TdxFetcher, FREQ_1MIN, FREQ_5MIN
from src.db import dao
from src.utils.logger import logger


_thread_local = threading.local()


def _get_worker_fetcher() -> TdxFetcher:
    """每个 worker 线程复用一个 fetcher，避免每只股票都重新 connect 并刷日志。"""
    fetcher = getattr(_thread_local, "fetcher", None)
    if fetcher is None:
        fetcher = TdxFetcher(mode="full")
        if not fetcher.connect():
            raise ConnectionError("无法连接 TDX 服务器")
        _thread_local.fetcher = fetcher
    return fetcher


def run_full_sync(stage: str = "all"):
    """
    执行全量铺底同步。

    stage:
    - all: 全部阶段（自动跳过已完成 xdxr）
    - stock_list
    - xdxr
    - 5min
    - 1min
    - resume: 等同 all，但语义上明确表示续跑
    """
    logger.info("=" * 60)
    logger.info(f"开始全量铺底同步, stage={stage}")
    logger.info("=" * 60)

    with TdxFetcher(mode="full") as fetcher:
        if stage in ("all", "resume", "stock_list", "xdxr", "5min", "1min"):
            _sync_stock_list(fetcher)

        if stage == "stock_list":
            logger.info("仅执行股票列表同步，结束")
            return

        stocks = dao.get_all_stocks()
        a_shares = [s for s in stocks if s["stock_type"] == 0]
        logger.info(f"待同步A股数量: {len(a_shares)}")

        if stage in ("all", "resume", "xdxr"):
            if dao.is_stage_completed("full", "xdxr") and stage in ("all", "resume"):
                logger.info("检测到 xdxr 已成功完成过，本次跳过")
            else:
                _sync_all_xdxr(fetcher, a_shares)
            if stage == "xdxr":
                logger.info("仅执行 xdxr 同步，结束")
                return

        if stage in ("all", "resume", "5min"):
            _sync_all_kline(a_shares, "kline_5min", FREQ_5MIN, "5min")
            if stage == "5min":
                logger.info("仅执行 5min 全量同步，结束")
                return

        if stage in ("all", "resume", "1min"):
            _sync_all_kline(a_shares, "kline_1min", FREQ_1MIN, "1min")
            if stage == "1min":
                logger.info("仅执行 1min 全量同步，结束")
                return

    logger.info("=" * 60)
    logger.info("全量铺底同步完成")
    logger.info("=" * 60)


def _sync_stock_list(fetcher: TdxFetcher):
    """同步股票列表"""
    log_id = dao.create_sync_log("full", "stock_list")
    try:
        stocks = fetcher.fetch_stock_list()
        dao.upsert_stock_info(stocks)
        dao.finish_sync_log(log_id, len(stocks), "success")
        logger.info(f"股票列表同步完成: {len(stocks)} 条")
    except Exception as e:
        dao.finish_sync_log(log_id, 0, "failed", str(e))
        logger.error(f"股票列表同步失败: {e}")
        raise


def _sync_all_xdxr(fetcher: TdxFetcher, stocks: list[dict]):
    """同步所有股票的除权除息事件"""
    log_id = dao.create_sync_log("full", "xdxr")
    total_rows = 0
    failed = 0

    for i, stock in enumerate(stocks):
        code, market = stock["stock_code"], stock["market"]
        try:
            rows = fetcher.fetch_xdxr(market, code)
            if rows:
                dao.batch_upsert_xdxr(rows)
                total_rows += len(rows)
        except Exception as e:
            failed += 1
            logger.warning(f"[{code}] 除权除息同步失败: {e}")

        if (i + 1) % 500 == 0:
            logger.info(f"除权除息进度: {i + 1}/{len(stocks)}, 累计 {total_rows} 条")

    dao.finish_sync_log(log_id, total_rows, "success")
    logger.info(f"除权除息同步完成: {total_rows} 条, 失败 {failed} 只")


def _target_bar_count(frequency: int) -> int:
    """根据频率返回目标bar数量阈值（按交易时段估算）"""
    # 用户口径：1min 每天 60*4=240 根，5min 每天 12*4=48 根
    if frequency == FREQ_1MIN:
        return int(240 * 100 * 0.9)  # 100天，留10%余量
    return int(48 * 700 * 0.9)       # 700天，留10%余量


def _fetch_and_save_one(stock: dict, table: str, frequency: int, target_count: int) -> tuple[str, int, int, bool, str | None]:
    """
    单只股票全量同步任务。
    返回 (stock_code, market, inserted_rows, skipped, error_msg)
    """
    code, market = stock["stock_code"], stock["market"]
    try:
        existing_count = dao.get_record_count(table, code, market)
        if existing_count >= target_count:
            return code, market, 0, True, None

        fetcher = _get_worker_fetcher()
        bars = fetcher.fetch_kline(frequency, market, code)
        inserted = dao.batch_upsert_kline(table, bars) if bars else 0
        return code, market, inserted, False, None
    except Exception as e:
        return code, market, 0, False, str(e)


def _sync_all_kline(stocks: list[dict], table: str, frequency: int, label: str):
    """
    逐只同步K线数据。

    热修策略：
    - 不再启动时扫大表做 completed_set 聚合，避免卡死
    - 对每只股票单独查 COUNT(*)
    - 若 count 达到目标阈值，则直接跳过
    - 若 count 不足，再抓取并依赖 INSERT IGNORE 补齐剩余部分

    这样既避免 completed_set 大 SQL，又不会对已完成股票全部从头网络重抓。
    """
    log_id = dao.create_sync_log("full", label)
    total_rows = 0
    synced_count = 0
    skipped_count = 0
    failed_count = 0

    sync_cfg = get_config().get("sync", {})
    full_workers = sync_cfg.get("full_workers", sync_cfg.get("workers", 1))
    progress_log_interval = sync_cfg.get("full_progress_log_interval", 20)
    target_count = _target_bar_count(frequency)

    logger.info(f"[{label}] 按单股 count 阈值判断续跑, target_count={target_count}, workers={full_workers}")
    logger.info(f"[{label}] 待检查股票数: {len(stocks)}")

    with ThreadPoolExecutor(max_workers=full_workers) as executor:
        futures = [executor.submit(_fetch_and_save_one, stock, table, frequency, target_count) for stock in stocks]
        logger.info(f"[{label}] 已提交 {len(futures)} 个任务到线程池")

        for idx, future in enumerate(as_completed(futures), start=1):
            code, market, inserted, skipped, error_msg = future.result()
            if error_msg:
                failed_count += 1
                logger.warning(f"[{label}][{code}] K线同步失败: {error_msg}")
            elif skipped:
                skipped_count += 1
            else:
                synced_count += 1
                total_rows += inserted

            if idx % progress_log_interval == 0 or idx == len(stocks):
                logger.info(
                    f"[{label}] 进度: {idx}/{len(stocks)}, "
                    f"已同步 {synced_count}, 已跳过 {skipped_count}, 失败 {failed_count}, "
                    f"累计新增 {total_rows} 条"
                )

    dao.finish_sync_log(log_id, total_rows, "success")
    logger.info(
        f"[{label}] 全量K线同步完成: "
        f"同步 {synced_count} 只, 跳过 {skipped_count} 只, 失败 {failed_count} 只, "
        f"累计新增 {total_rows} 条"
    )
