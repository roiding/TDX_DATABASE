"""
TDX Database — 主入口

用法:
    python main.py init         初始化数据库表
    python main.py full         全量铺底同步（首次使用）
    python main.py daily        每日增量同步
    python main.py status       查看同步状态
"""

import sys
import click
from pathlib import Path

# 确保项目根目录在 sys.path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import load_config
from src.utils.logger import setup_logger, logger


@click.group()
@click.option("--config", "-c", default="config.yaml", help="配置文件路径")
def cli(config):
    """TDX 股票分钟数据采集入库工具"""
    load_config(config)
    setup_logger()


@cli.command()
def init():
    """初始化数据库（建表）"""
    from src.db.schema import init_database
    init_database()
    click.echo("数据库初始化完成")


@cli.command()
def full():
    """全量铺底同步（首次使用）"""
    from src.sync.full_sync import run_full_sync
    click.echo("开始全量铺底同步，预计需要较长时间...")
    run_full_sync()
    click.echo("全量铺底同步完成")


@cli.command()
def daily():
    """每日增量同步"""
    from src.sync.daily_sync import run_daily_sync
    click.echo("开始每日增量同步...")
    run_daily_sync()
    click.echo("每日增量同步完成")


@cli.command()
def status():
    """查看最近的同步记录"""
    from src.db.connection import fetchall
    rows = fetchall("""
        SELECT id, sync_type, data_type, stock_code, start_time, end_time,
               rows_synced, status, error_msg
        FROM sync_log
        ORDER BY id DESC
        LIMIT 20
    """)
    if not rows:
        click.echo("暂无同步记录")
        return

    click.echo(f"{'ID':>5} {'类型':<8} {'数据':<8} {'股票':<8} {'状态':<8} {'行数':>10} {'开始时间':<20} {'耗时'}")
    click.echo("-" * 90)
    for r in rows:
        elapsed = ""
        if r["end_time"] and r["start_time"]:
            delta = r["end_time"] - r["start_time"]
            elapsed = str(delta).split(".")[0]
        click.echo(
            f"{r['id']:>5} {r['sync_type']:<8} {r['data_type']:<8} "
            f"{r['stock_code'] or 'ALL':<8} {r['status']:<8} {r['rows_synced']:>10} "
            f"{str(r['start_time'])[:19]:<20} {elapsed}"
        )


@cli.command()
@click.argument("stock_code")
@click.option("--market", "-m", type=int, default=None, help="市场: 0=深圳, 1=上海（自动识别）")
@click.option("--freq", "-f", type=click.Choice(["1min", "5min"]), default="5min", help="K线周期")
@click.option("--limit", "-n", type=int, default=20, help="显示条数")
def query(stock_code, market, freq, limit):
    """查询某只股票的K线数据（调试用）"""
    from src.db.connection import fetchall

    if market is None:
        market = 1 if stock_code.startswith(("6", "5")) else 0

    table = "kline_1min" if freq == "1min" else "kline_5min"
    rows = fetchall(
        f"SELECT * FROM {table} WHERE stock_code=%s AND market=%s ORDER BY dt DESC LIMIT %s",
        (stock_code, market, limit),
    )

    if not rows:
        click.echo(f"无数据: {stock_code} ({freq})")
        return

    click.echo(f"{'时间':<20} {'开盘':>10} {'最高':>10} {'最低':>10} {'收盘':>10} {'成交量':>12} {'成交额':>14}")
    click.echo("-" * 100)
    for r in rows:
        click.echo(
            f"{str(r['dt']):<20} {r['open']:>10.3f} {r['high']:>10.3f} "
            f"{r['low']:>10.3f} {r['close']:>10.3f} {r['volume']:>12} {r['amount']:>14.2f}"
        )


if __name__ == "__main__":
    cli()
