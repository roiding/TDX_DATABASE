"""
全量同步 — 首次铺底

流程：
1. 拉取全部A股列表并入库
2. 拉取所有除权除息事件并入库
3. 逐只股票拉取全量1min/5min数据并入库
4. 全程记录同步日志，支持断点续传（跳过已同步的股票）
"""

from datetime import datetime
from src.config import get_config
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
    断点续传：查已有记录数作为offset起点，从断点继续往回翻页拉取更早的历史数据。
    如果已有记录数已达到服务器上限（翻完所有页），则跳过。
    """
    log_id = dao.create_sync_log("full", label)
    total_rows = 0
    synced_count = 0
    skipped_count = 0
    failed_count = 0

    # 服务器可用数据上限：max_pages * batch_size
    cfg = get_config().get("sync", {})
    max_bars = cfg.get("max_pages", 20) * cfg.get("batch_size", 800)

    for i, stock in enumerate(stocks):
        code, market = stock["stock_code"], stock["market"]

        # 查已有记录数
        existing_count = dao.get_record_count(table, code, market)

        # 记录数已达上限，说明之前已拉完，跳过
        if existing_count >= max_bars:
            skipped_count += 1
            if (i + 1) % 500 == 0:
                logger.info(f"[{label}] 进度: {i + 1}/{len(stocks)}, 已同步 {synced_count}, 跳过 {skipped_count}")
            continue

        try:
            # 从已有记录数处继续往回翻页，INSERT IGNORE处理重叠部分
            bars = fetcher.fetch_kline(frequency, market, code, start_offset=existing_count)
            if bars:
                inserted = dao.batch_upsert_kline(table, bars)
                total_rows += inserted
                synced_count += 1
            elif existing_count > 0:
                # 没拿到新数据但已有部分数据，说明服务器数据到头了，也算完成
                skipped_count += 1
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
