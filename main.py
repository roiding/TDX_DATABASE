"""
TDX Database — Web 服务入口

启动即为长驻服务：
- 自动建表
- 定时每日增量同步（默认16:30）
- 提供 REST API 查询数据、手动触发同步
"""

import sys
import threading
from pathlib import Path
from contextlib import asynccontextmanager

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, Query, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler

from src.config import load_config, get_config
from src.utils.logger import setup_logger, logger
from src.db.schema import init_database
from src.db.connection import fetchall, fetchone


# ============================================================
# 全局状态
# ============================================================
_sync_lock = threading.Lock()
_sync_status = {"running": False, "type": None, "message": "idle"}


def _run_sync(sync_type: str):
    """在后台线程中执行同步，防止并发"""
    if not _sync_lock.acquire(blocking=False):
        logger.warning(f"同步任务已在运行，跳过本次 {sync_type}")
        return
    try:
        _sync_status.update(running=True, type=sync_type, message="running")
        if sync_type == "full":
            from src.sync.full_sync import run_full_sync
            run_full_sync()
        else:
            from src.sync.daily_sync import run_daily_sync
            run_daily_sync()
        _sync_status.update(running=False, type=None, message=f"last {sync_type} finished")
    except Exception as e:
        logger.error(f"同步异常: {e}")
        _sync_status.update(running=False, type=None, message=f"error: {e}")
    finally:
        _sync_lock.release()


# ============================================================
# App 生命周期
# ============================================================
scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_config()
    setup_logger()

    # 自动建表
    init_database()

    # 定时每日增量：工作日 16:30
    sync_cfg = get_config().get("sync", {})
    cron_hour = sync_cfg.get("daily_hour", 16)
    cron_minute = sync_cfg.get("daily_minute", 30)
    scheduler.add_job(
        lambda: _run_sync("daily"),
        "cron",
        hour=cron_hour, minute=cron_minute,
        day_of_week="mon-fri",
        id="daily_sync",
    )
    scheduler.start()
    logger.info(f"定时任务已启动: 每工作日 {cron_hour}:{cron_minute:02d} 增量同步")

    yield

    scheduler.shutdown()


app = FastAPI(title="TDX Database", lifespan=lifespan)


# ============================================================
# 同步控制 API
# ============================================================

@app.post("/api/sync/full")
def trigger_full_sync():
    """手动触发全量铺底"""
    if _sync_status["running"]:
        raise HTTPException(409, f"同步任务正在运行: {_sync_status['type']}")
    threading.Thread(target=_run_sync, args=("full",), daemon=True).start()
    return {"message": "全量同步已启动"}


@app.post("/api/sync/daily")
def trigger_daily_sync():
    """手动触发每日增量"""
    if _sync_status["running"]:
        raise HTTPException(409, f"同步任务正在运行: {_sync_status['type']}")
    threading.Thread(target=_run_sync, args=("daily",), daemon=True).start()
    return {"message": "增量同步已启动"}


@app.get("/api/sync/status")
def sync_status():
    """查询同步状态 + 最近同步记录"""
    logs = fetchall("""
        SELECT id, sync_type, data_type, stock_code, start_time, end_time,
               rows_synced, status, error_msg
        FROM sync_log ORDER BY id DESC LIMIT 10
    """)
    return {
        "current": _sync_status,
        "recent_logs": logs,
    }


# ============================================================
# 数据查询 API
# ============================================================

@app.get("/api/stocks")
def list_stocks(stock_type: int = None):
    """获取股票列表。stock_type: 0=股票, 1=指数, 2=ETF"""
    sql = "SELECT stock_code, market, stock_name, stock_type FROM stock_info"
    params = ()
    if stock_type is not None:
        sql += " WHERE stock_type=%s"
        params = (stock_type,)
    sql += " ORDER BY market, stock_code"
    return fetchall(sql, params)


@app.get("/api/kline/{stock_code}")
def get_kline(
    stock_code: str,
    freq: str = Query("5min", regex="^(1min|5min)$"),
    start: str = Query(None, description="开始时间 YYYY-MM-DD HH:MM"),
    end: str = Query(None, description="结束时间 YYYY-MM-DD HH:MM"),
    limit: int = Query(1000, ge=1, le=10000),
):
    """查询K线数据（原始不复权）"""
    market = 1 if stock_code.startswith(("6", "5")) else 0
    table = "kline_1min" if freq == "1min" else "kline_5min"

    conditions = ["stock_code=%s", "market=%s"]
    params = [stock_code, market]

    if start:
        conditions.append("dt>=%s")
        params.append(start)
    if end:
        conditions.append("dt<=%s")
        params.append(end)

    sql = f"SELECT dt, open, high, low, close, volume, amount FROM {table} WHERE {' AND '.join(conditions)} ORDER BY dt DESC LIMIT %s"
    params.append(limit)

    rows = fetchall(sql, tuple(params))
    return {"stock_code": stock_code, "freq": freq, "count": len(rows), "data": rows}


@app.get("/api/xdxr/{stock_code}")
def get_xdxr(stock_code: str):
    """查询除权除息事件"""
    market = 1 if stock_code.startswith(("6", "5")) else 0
    rows = fetchall(
        "SELECT * FROM xdxr_event WHERE stock_code=%s AND market=%s ORDER BY ex_date DESC",
        (stock_code, market),
    )
    return {"stock_code": stock_code, "count": len(rows), "data": rows}


@app.get("/api/stats")
def get_stats():
    """数据库统计"""
    stats = {}
    for table in ["kline_1min", "kline_5min", "xdxr_event", "stock_info"]:
        row = fetchone(f"SELECT COUNT(*) as cnt FROM {table}")
        stats[table] = row["cnt"] if row else 0

    for table in ["kline_1min", "kline_5min"]:
        row = fetchone(f"SELECT MIN(dt) as min_dt, MAX(dt) as max_dt FROM {table}")
        if row and row["min_dt"]:
            stats[f"{table}_range"] = f"{row['min_dt']} ~ {row['max_dt']}"

    return stats


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
