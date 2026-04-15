"""
MySQL 表结构定义

设计原则：
1. K线表只存原始数据（不复权），因为每次除权除息事件发生后，前复权/后复权价格会变化
2. 除权除息事件单独存表，复权因子由应用层按需计算
3. K线表按月分区，兼顾查询效率和分区管理
4. stock_code + datetime 作为联合主键，天然去重
"""

from src.db.connection import get_conn
from src.utils.logger import logger

# ============================================================
# 表结构定义
# ============================================================

DDL_STOCK_INFO = """
CREATE TABLE IF NOT EXISTS stock_info (
    stock_code  VARCHAR(10)  NOT NULL COMMENT '股票代码，如 000001',
    market      TINYINT      NOT NULL COMMENT '0=深圳 1=上海',
    stock_name  VARCHAR(20)  NOT NULL DEFAULT '' COMMENT '股票名称',
    stock_type  TINYINT      NOT NULL DEFAULT 0 COMMENT '0=股票 1=指数 2=ETF 3=债券',
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, market)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基础信息';
"""

# ---- 1分钟K线（原始不复权数据） ----
# 按月分区：1min数据量最大，按月分区有利于过期数据清理和查询效率
DDL_KLINE_1MIN = """
CREATE TABLE IF NOT EXISTS kline_1min (
    stock_code  VARCHAR(10)    NOT NULL COMMENT '股票代码',
    market      TINYINT        NOT NULL COMMENT '0=深圳 1=上海',
    dt          DATETIME       NOT NULL COMMENT 'K线时间',
    open        DECIMAL(10,3)  NOT NULL COMMENT '开盘价（原始不复权）',
    high        DECIMAL(10,3)  NOT NULL COMMENT '最高价',
    low         DECIMAL(10,3)  NOT NULL COMMENT '最低价',
    close       DECIMAL(10,3)  NOT NULL COMMENT '收盘价',
    volume      BIGINT         NOT NULL COMMENT '成交量（股）',
    amount      DECIMAL(16,2)  NOT NULL COMMENT '成交额（元）',
    PRIMARY KEY (stock_code, dt, market)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='1分钟K线（原始不复权）'
PARTITION BY RANGE (TO_DAYS(dt)) (
    PARTITION p2023q1 VALUES LESS THAN (TO_DAYS('2023-04-01')),
    PARTITION p2023q2 VALUES LESS THAN (TO_DAYS('2023-07-01')),
    PARTITION p2023q3 VALUES LESS THAN (TO_DAYS('2023-10-01')),
    PARTITION p2023q4 VALUES LESS THAN (TO_DAYS('2024-01-01')),
    PARTITION p2024q1 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p2024q2 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p2024q3 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p2024q4 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p2025q1 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p2025q2 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p2025q3 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p2025q4 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p2026q1 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026q2 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026q3 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026q4 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
"""

# ---- 5分钟K线（原始不复权数据） ----
DDL_KLINE_5MIN = """
CREATE TABLE IF NOT EXISTS kline_5min (
    stock_code  VARCHAR(10)    NOT NULL COMMENT '股票代码',
    market      TINYINT        NOT NULL COMMENT '0=深圳 1=上海',
    dt          DATETIME       NOT NULL COMMENT 'K线时间',
    open        DECIMAL(10,3)  NOT NULL COMMENT '开盘价（原始不复权）',
    high        DECIMAL(10,3)  NOT NULL COMMENT '最高价',
    low         DECIMAL(10,3)  NOT NULL COMMENT '最低价',
    close       DECIMAL(10,3)  NOT NULL COMMENT '收盘价',
    volume      BIGINT         NOT NULL COMMENT '成交量（股）',
    amount      DECIMAL(16,2)  NOT NULL COMMENT '成交额（元）',
    PRIMARY KEY (stock_code, dt, market)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='5分钟K线（原始不复权）'
PARTITION BY RANGE (TO_DAYS(dt)) (
    PARTITION p2023q1 VALUES LESS THAN (TO_DAYS('2023-04-01')),
    PARTITION p2023q2 VALUES LESS THAN (TO_DAYS('2023-07-01')),
    PARTITION p2023q3 VALUES LESS THAN (TO_DAYS('2023-10-01')),
    PARTITION p2023q4 VALUES LESS THAN (TO_DAYS('2024-01-01')),
    PARTITION p2024q1 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION p2024q2 VALUES LESS THAN (TO_DAYS('2024-07-01')),
    PARTITION p2024q3 VALUES LESS THAN (TO_DAYS('2024-10-01')),
    PARTITION p2024q4 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p2025q1 VALUES LESS THAN (TO_DAYS('2025-04-01')),
    PARTITION p2025q2 VALUES LESS THAN (TO_DAYS('2025-07-01')),
    PARTITION p2025q3 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p2025q4 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p2026q1 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026q2 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026q3 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026q4 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
"""

# ---- 除权除息事件表 ----
# 存储原始事件数据，复权因子由应用层计算
DDL_XDXR_EVENT = """
CREATE TABLE IF NOT EXISTS xdxr_event (
    stock_code      VARCHAR(10)     NOT NULL COMMENT '股票代码',
    market          TINYINT         NOT NULL COMMENT '0=深圳 1=上海',
    ex_date         DATE            NOT NULL COMMENT '除权除息日',
    category        TINYINT         NOT NULL DEFAULT 0 COMMENT '事件类型: 1=除权除息 2=送配股上市 3=非流通股上市 4=未知 5=股本变化 6=增发新股 7=股改 8=增发新股上市 9=转配股上市 10=可转债上市 11=扩缩股 12=非流通股缩股 13=送认购权证 14=送认沽权证',
    fenhong         DECIMAL(12,4)   NOT NULL DEFAULT 0 COMMENT '每10股分红（元）',
    peigujia        DECIMAL(10,3)   NOT NULL DEFAULT 0 COMMENT '配股价（元）',
    songzhuangu     DECIMAL(12,4)   NOT NULL DEFAULT 0 COMMENT '每10股送转股（股）',
    peigu           DECIMAL(12,4)   NOT NULL DEFAULT 0 COMMENT '每10股配股（股）',
    suogu           DECIMAL(12,4)   NOT NULL DEFAULT 0 COMMENT '缩股比例',
    panqianliutong  DECIMAL(16,2)   NOT NULL DEFAULT 0 COMMENT '盘前流通股本（万股）',
    panhouliutong   DECIMAL(16,2)   NOT NULL DEFAULT 0 COMMENT '盘后流通股本（万股）',
    qianzongguben   DECIMAL(16,2)   NOT NULL DEFAULT 0 COMMENT '前总股本（万股）',
    houzongguben    DECIMAL(16,2)   NOT NULL DEFAULT 0 COMMENT '后总股本（万股）',
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code, market, ex_date, category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='除权除息事件（原始数据）';
"""

# ---- 同步日志 ----
DDL_SYNC_LOG = """
CREATE TABLE IF NOT EXISTS sync_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    sync_type   VARCHAR(20)  NOT NULL COMMENT 'full / daily',
    data_type   VARCHAR(10)  NOT NULL COMMENT '1min / 5min / xdxr / stock_list',
    stock_code  VARCHAR(10)  DEFAULT NULL COMMENT '具体股票代码，NULL表示全局',
    market      TINYINT      DEFAULT NULL,
    start_time  DATETIME     NOT NULL,
    end_time    DATETIME     DEFAULT NULL,
    rows_synced INT          NOT NULL DEFAULT 0,
    status      VARCHAR(10)  NOT NULL DEFAULT 'running' COMMENT 'running / success / failed',
    error_msg   TEXT         DEFAULT NULL,
    INDEX idx_type_status (sync_type, data_type, status),
    INDEX idx_stock (stock_code, market, data_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步日志';
"""

ALL_DDL = [DDL_STOCK_INFO, DDL_KLINE_1MIN, DDL_KLINE_5MIN, DDL_XDXR_EVENT, DDL_SYNC_LOG]


def init_database():
    """建库建表，幂等操作"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for ddl in ALL_DDL:
                cur.execute(ddl)
            conn.commit()
        logger.info("数据库表初始化完成")
    finally:
        conn.close()


def add_partition_if_needed(table: str, partition_name: str, less_than: str):
    """动态添加分区（未来扩展用）"""
    sql = f"""
        ALTER TABLE {table}
        REORGANIZE PARTITION p_future INTO (
            PARTITION {partition_name} VALUES LESS THAN (TO_DAYS('{less_than}')),
            PARTITION p_future VALUES LESS THAN MAXVALUE
        )
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info(f"已为 {table} 添加分区 {partition_name}")
    except Exception as e:
        logger.warning(f"添加分区失败（可能已存在）: {e}")
    finally:
        conn.close()
