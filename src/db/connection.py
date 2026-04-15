from __future__ import annotations

import pymysql
from dbutils.pooled_db import PooledDB
from src.config import get_config

_pool = None


def get_pool() -> PooledDB:
    global _pool
    if _pool is not None:
        return _pool

    cfg = get_config()["mysql"]
    _pool = PooledDB(
        creator=pymysql,
        maxconnections=cfg.get("pool_size", 5),
        mincached=1,
        maxcached=cfg.get("pool_size", 5),
        blocking=True,
        host=cfg["host"],
        port=cfg.get("port", 3306),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        autocommit=True,
    )
    return _pool


def get_conn():
    return get_pool().connection()


def execute(sql: str, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def execute_many(sql: str, params_list: list):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, params_list)
        conn.commit()
    finally:
        conn.close()


def fetchall(sql: str, params=None) -> list:
    conn = get_conn()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def fetchone(sql: str, params=None) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()
