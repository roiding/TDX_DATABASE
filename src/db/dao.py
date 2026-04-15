"""
数据访问层 — 批量写入K线、除权除息、同步日志
使用 INSERT IGNORE 天然去重（联合主键冲突时跳过）
"""
from __future__ import annotations

from datetime import datetime
from src.db.connection import get_conn
from src.utils.logger import logger


def batch_upsert_kline(table: str, rows: list[dict], batch_size: int = 5000):
    """
    批量写入K线数据。
    rows: [{'stock_code', 'market', 'dt', 'open', 'high', 'low', 'close', 'volume', 'amount'}, ...]
    使用 INSERT IGNORE —— 主键重复则跳过，保证幂等。
    """
    if not rows:
        return 0

    sql = f"""
        INSERT IGNORE INTO {table}
            (stock_code, market, dt, open, high, low, close, volume, amount)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    total_inserted = 0
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                chunk = rows[i : i + batch_size]
                params = [
                    (
                        r["stock_code"], r["market"], r["dt"],
                        r["open"], r["high"], r["low"], r["close"],
                        r["volume"], r["amount"],
                    )
                    for r in chunk
                ]
                cur.executemany(sql, params)
                total_inserted += cur.rowcount
            conn.commit()
    finally:
        conn.close()

    return total_inserted


def batch_upsert_xdxr(rows: list[dict]):
    """批量写入除权除息事件，ON DUPLICATE KEY UPDATE 覆盖更新"""
    if not rows:
        return 0

    sql = """
        INSERT INTO xdxr_event
            (stock_code, market, ex_date, category,
             fenhong, peigujia, songzhuangu, peigu, suogu,
             panqianliutong, panhouliutong, qianzongguben, houzongguben)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            fenhong=VALUES(fenhong), peigujia=VALUES(peigujia),
            songzhuangu=VALUES(songzhuangu), peigu=VALUES(peigu),
            suogu=VALUES(suogu),
            panqianliutong=VALUES(panqianliutong), panhouliutong=VALUES(panhouliutong),
            qianzongguben=VALUES(qianzongguben), houzongguben=VALUES(houzongguben)
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            params = [
                (
                    r["stock_code"], r["market"], r["ex_date"], r.get("category", 1),
                    r.get("fenhong", 0), r.get("peigujia", 0),
                    r.get("songzhuangu", 0), r.get("peigu", 0), r.get("suogu", 0),
                    r.get("panqianliutong", 0), r.get("panhouliutong", 0),
                    r.get("qianzongguben", 0), r.get("houzongguben", 0),
                )
                for r in rows
            ]
            cur.executemany(sql, params)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def upsert_stock_info(rows: list[dict]):
    """批量写入/更新股票基础信息"""
    if not rows:
        return

    sql = """
        INSERT INTO stock_info (stock_code, market, stock_name, stock_type)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE stock_name=VALUES(stock_name), stock_type=VALUES(stock_type)
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            params = [(r["stock_code"], r["market"], r.get("stock_name", ""), r.get("stock_type", 0)) for r in rows]
            cur.executemany(sql, params)
            conn.commit()
    finally:
        conn.close()


def get_all_stocks() -> list[dict]:
    """获取所有已入库的股票列表"""
    from src.db.connection import fetchall
    return fetchall("SELECT stock_code, market, stock_name, stock_type FROM stock_info ORDER BY market, stock_code")


def get_latest_dt(table: str, stock_code: str, market: int) -> datetime | None:
    """查询某只股票在某张K线表中的最新时间"""
    from src.db.connection import fetchone
    row = fetchone(
        f"SELECT MAX(dt) as max_dt FROM {table} WHERE stock_code=%s AND market=%s",
        (stock_code, market),
    )
    return row["max_dt"] if row and row["max_dt"] else None


def get_oldest_dt(table: str, stock_code: str, market: int) -> datetime | None:
    """查询某只股票在某张K线表中的最早时间"""
    from src.db.connection import fetchone
    row = fetchone(
        f"SELECT MIN(dt) as min_dt FROM {table} WHERE stock_code=%s AND market=%s",
        (stock_code, market),
    )
    return row["min_dt"] if row and row["min_dt"] else None




def is_stage_completed(sync_type: str, data_type: str) -> bool:
    """判断某个同步阶段是否已成功完成过"""
    from src.db.connection import fetchone
    row = fetchone(
        """
        SELECT id FROM sync_log
        WHERE sync_type=%s AND data_type=%s AND status='success'
        ORDER BY id DESC LIMIT 1
        """,
        (sync_type, data_type),
    )
    return row is not None


def get_latest_sync_log(sync_type: str, data_type: str) -> dict | None:
    """获取某阶段最近一条同步日志"""
    from src.db.connection import fetchone
    return fetchone(
        """
        SELECT id, sync_type, data_type, stock_code, market, start_time, end_time,
               rows_synced, status, error_msg
        FROM sync_log
        WHERE sync_type=%s AND data_type=%s
        ORDER BY id DESC LIMIT 1
        """,
        (sync_type, data_type),
    )


def get_completed_stock_set(table: str, min_days_back: int) -> set[tuple[str, int]]:
    """
    一次性取出已完成全量铺底的股票集合。
    条件：该股票最早数据距今达到阈值天数。
    返回 {(stock_code, market), ...}
    """
    from src.db.connection import fetchall
    rows = fetchall(
        f"""
        SELECT stock_code, market
        FROM {table}
        GROUP BY stock_code, market
        HAVING DATEDIFF(NOW(), MIN(dt)) >= %s
        """,
        (min_days_back,),
    )
    return {(r['stock_code'], r['market']) for r in rows}


def create_sync_log(sync_type: str, data_type: str, stock_code: str = None, market: int = None) -> int:
    """创建一条同步日志，返回 log id"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sync_log (sync_type, data_type, stock_code, market, start_time, status)
                   VALUES (%s, %s, %s, %s, %s, 'running')""",
                (sync_type, data_type, stock_code, market, datetime.now()),
            )
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()


def finish_sync_log(log_id: int, rows_synced: int, status: str = "success", error_msg: str = None):
    """更新同步日志状态"""
    from src.db.connection import execute
    execute(
        """UPDATE sync_log SET end_time=%s, rows_synced=%s, status=%s, error_msg=%s WHERE id=%s""",
        (datetime.now(), rows_synced, status, error_msg, log_id),
    )
