"""
每日增量同步

流程：
1. 更新股票列表（发现新上市股票）
2. 更新除权除息事件
3. 逐只股票增量拉取最新K线数据
   - 查询该股票在DB中的最新时间
   - 从pytdx拉取数据时，遇到已有时间则停止
   - INSERT IGNORE 保证幂等

设计为可重复执行 — 同一天多次执行不会产生重复数据
"""

from src.fetcher.tdx_fetcher import TdxFetcher, FREQ_1MIN, FREQ_5MIN
from src.db import dao
from src.utils.logger import logger


def run_daily_sync():
    """执行每日增量同步"""
    logger.info("=" * 60)
    logger.info("开始每日增量同步")
    logger.info("=" * 60)

    with TdxFetcher() as fetcher:
        # Step 1: 更新股票列表
        _update_stock_list(fetcher)

        # Step 2: 获取A股列表
        stocks = dao.get_all_stocks()
        a_shares = [s for s in stocks if s["stock_type"] == 0]
        logger.info(f"待增量同步A股数量: {len(a_shares)}")

        # Step 3: 增量同步除权除息
        _update_xdxr(fetcher, a_shares)

        # Step 4: 增量同步K线
        _incremental_kline(fetcher, a_shares, "kline_5min", FREQ_5MIN, "5min")
        _incremental_kline(fetcher, a_shares, "kline_1min", FREQ_1MIN, "1min")

    logger.info("=" * 60)
    logger.info("每日增量同步完成")
    logger.info("=" * 60)


def _update_stock_list(fetcher: TdxFetcher):
    """更新股票列表（发现新股）"""
    log_id = dao.create_sync_log("daily", "stock_list")
    try:
        stocks = fetcher.fetch_stock_list()
        dao.upsert_stock_info(stocks)
        dao.finish_sync_log(log_id, len(stocks), "success")
        logger.info(f"股票列表更新完成: {len(stocks)} 条")
    except Exception as e:
        dao.finish_sync_log(log_id, 0, "failed", str(e))
        logger.error(f"股票列表更新失败: {e}")


def _update_xdxr(fetcher: TdxFetcher, stocks: list[dict]):
    """增量更新除权除息事件（全量覆盖写入，由 ON DUPLICATE KEY UPDATE 去重）"""
    log_id = dao.create_sync_log("daily", "xdxr")
    total_rows = 0

    for i, stock in enumerate(stocks):
        code, market = stock["stock_code"], stock["market"]
        try:
            rows = fetcher.fetch_xdxr(market, code)
            if rows:
                dao.batch_upsert_xdxr(rows)
                total_rows += len(rows)
        except Exception as e:
            logger.warning(f"[{code}] 除权除息更新失败: {e}")

        if (i + 1) % 500 == 0:
            logger.info(f"除权除息更新进度: {i + 1}/{len(stocks)}")

    dao.finish_sync_log(log_id, total_rows, "success")
    logger.info(f"除权除息更新完成: {total_rows} 条")


def _incremental_kline(
    fetcher: TdxFetcher,
    stocks: list[dict],
    table: str,
    frequency: int,
    label: str,
):
    """
    增量同步K线数据。
    对每只股票：查询DB最新时间 → 拉取数据到该时间为止 → INSERT IGNORE 去重写入。
    增量模式只翻1-2页（最近800-1600条），足够覆盖1天的新数据。
    """
    log_id = dao.create_sync_log("daily", label)
    total_rows = 0
    synced_count = 0
    failed_count = 0

    for i, stock in enumerate(stocks):
        code, market = stock["stock_code"], stock["market"]

        # 查询已有的最新时间
        latest_dt = dao.get_latest_dt(table, code, market)

        try:
            # 增量模式：最多翻2页，遇到已有数据停止
            bars = fetcher.fetch_kline(
                frequency, market, code,
                max_pages=2,
                stop_before=latest_dt,
            )
            if bars:
                inserted = dao.batch_upsert_kline(table, bars)
                total_rows += inserted
                synced_count += 1
        except Exception as e:
            failed_count += 1
            logger.warning(f"[{label}][{code}] 增量同步失败: {e}")

        if (i + 1) % 500 == 0:
            logger.info(
                f"[{label}] 增量进度: {i + 1}/{len(stocks)}, "
                f"新增 {total_rows} 条, 失败 {failed_count}"
            )

    dao.finish_sync_log(log_id, total_rows, "success")
    logger.info(
        f"[{label}] 增量同步完成: "
        f"更新 {synced_count} 只, 失败 {failed_count} 只, "
        f"新增 {total_rows} 条"
    )
