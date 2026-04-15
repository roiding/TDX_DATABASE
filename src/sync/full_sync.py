"""
全量同步 — 首次铺底

流程：
1. 拉取全部A股列表并入库
2. 拉取所有除权除息事件并入库
3. 逐只股票拉取全量1min/5min数据并入库
4. 全程记录同步日志，支持断点续传（跳过已同步的股票）
"""

from datetime import datetime
from src.fetcher.tdx_fetcher import TdxFetcher, FREQ_1MIN, FREQ_5MIN
from src.db import dao
from src.utils.logger import logger


def run_full_sync():
    """执行全量铺底同步"""
    logger.info("=" * 60)
    logger.info("开始全量铺底同步")
    logger.info("=" * 60)

    with TdxFetcher() as fetcher:
        # Step 1: 同步股票列表
        _sync_stock_list(fetcher)

        # Step 2: 获取已入库的股票列表
        stocks = dao.get_all_stocks()
        # 只同步股票（type=0），不同步指数和ETF的分钟数据量过大
        a_shares = [s for s in stocks if s["stock_type"] == 0]
        logger.info(f"待同步A股数量: {len(a_shares)}")

        # Step 3: 同步除权除息事件
        _sync_all_xdxr(fetcher, a_shares)

        # Step 4: 逐只同步K线数据
        _sync_all_kline(fetcher, a_shares, "kline_5min", FREQ_5MIN, "5min")
        _sync_all_kline(fetcher, a_shares, "kline_1min", FREQ_1MIN, "1min")

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

    status = "success" if failed == 0 else "success"  # 部分失败不影响整体
    dao.finish_sync_log(log_id, total_rows, status)
    logger.info(f"除权除息同步完成: {total_rows} 条, 失败 {failed} 只")


def _sync_all_kline(
    fetcher: TdxFetcher,
    stocks: list[dict],
    table: str,
    frequency: int,
    label: str,
):
    """
    逐只同步K线数据。
    断点续传逻辑：
    - 查该股票已有数据的最早时间(MIN(dt))
    - 如果最早时间距今已超过阈值（5min>=650天），说明已拉满，跳过
    - 否则从offset=0重新拉全量，INSERT IGNORE去重
    （不能用记录数做offset，因为TDX服务器是滑动窗口，每天有新增有淘汰）
    """
    log_id = dao.create_sync_log("full", label)
    total_rows = 0
    synced_count = 0
    skipped_count = 0
    failed_count = 0

    # 5min服务器保留~700天，1min保留~100天，留些余量判断
    from src.fetcher.tdx_fetcher import FREQ_5MIN
    days_threshold = 650 if frequency == FREQ_5MIN else 90

    for i, stock in enumerate(stocks):
        code, market = stock["stock_code"], stock["market"]

        # 查最早数据时间，判断是否已拉满
        oldest_dt = dao.get_oldest_dt(table, code, market)
        if oldest_dt is not None:
            days_back = (datetime.now() - oldest_dt).days
            if days_back >= days_threshold:
                skipped_count += 1
                if (i + 1) % 500 == 0:
                    logger.info(f"[{label}] 进度: {i + 1}/{len(stocks)}, 已同步 {synced_count}, 跳过 {skipped_count}")
                continue

        try:
            # 从offset=0拉全量，INSERT IGNORE处理与已有数据的重叠
            bars = fetcher.fetch_kline(frequency, market, code)
            if bars:
                inserted = dao.batch_upsert_kline(table, bars)
                total_rows += inserted
                synced_count += 1
            else:
                synced_count += 1  # 空数据（停牌股等）
        except Exception as e:
            failed_count += 1
            logger.warning(f"[{label}][{code}] K线同步失败: {e}")

        if (i + 1) % 100 == 0:
            logger.info(
                f"[{label}] 进度: {i + 1}/{len(stocks)}, "
                f"已同步 {synced_count}, 跳过 {skipped_count}, 失败 {failed_count}, "
                f"累计 {total_rows} 条"
            )

    dao.finish_sync_log(log_id, total_rows, "success")
    logger.info(
        f"[{label}] 全量K线同步完成: "
        f"同步 {synced_count} 只, 跳过 {skipped_count} 只, 失败 {failed_count} 只, "
        f"共 {total_rows} 条"
    )
