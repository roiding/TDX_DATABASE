# TDX Database

A股分钟级K线数据采集入库服务。从通达信行情服务器拉取 **1分钟** 和 **5分钟** 原始K线数据，存入MySQL，解决通达信本地数据保留期限问题（1分钟仅100天，5分钟仅700天）。

## 核心设计

- **只存原始不复权数据** — 除权除息事件独立存表，复权因子由应用层按需计算
- **Web 服务** — FastAPI 长驻服务，提供 REST API 查询数据、手动触发同步
- **自动定时同步** — 每工作日收盘后自动增量同步（默认16:30）
- **断点续传** — 全量同步中断后重启会从断点继续
- **幂等写入** — 重复执行不产生重复数据

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/stocks` | 股票列表 |
| GET | `/api/kline/{code}?freq=5min&start=...&end=...&limit=1000` | K线查询 |
| GET | `/api/xdxr/{code}` | 除权除息事件 |
| GET | `/api/stats` | 数据库统计 |
| POST | `/api/sync/full` | 兼容入口：全量铺底(all) |
| POST | `/api/sync/full/resume` | 恢复续跑，自动跳过已完成阶段 |
| POST | `/api/sync/full/xdxr` | 仅跑除权除息全量 |
| POST | `/api/sync/full/5min` | 仅跑5分钟全量 |
| POST | `/api/sync/full/1min` | 仅跑1分钟全量 |
| POST | `/api/sync/daily` | 触发增量同步 |
| GET | `/api/sync/status` | 同步状态 |
| GET | `/docs` | Swagger 文档 |

## Docker 部署

```bash
# docker-compose (推荐)
docker compose up -d

# 或直接 docker run
docker run -d -p 8000:8000 \
  -e MYSQL_HOST=xxx -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root -e MYSQL_PASSWORD=xxx \
  -e MYSQL_DATABASE=tdx_data \
  your-username/tdx-database:latest
```

启动后：
```bash
# 兼容入口：触发全量铺底（all）
curl -X POST http://localhost:8000/api/sync/full

# 恢复续跑（推荐）
curl -X POST http://localhost:8000/api/sync/full/resume

# 单独跑5分钟全量
curl -X POST http://localhost:8000/api/sync/full/5min

# 单独跑1分钟全量
curl -X POST http://localhost:8000/api/sync/full/1min

# 查看同步状态
curl http://localhost:8000/api/sync/status

# 查询数据
curl "http://localhost:8000/api/kline/600000?freq=5min&limit=10"
```

双机并行建议：
- 机器A：`/api/sync/full/5min`
- 机器B：`/api/sync/full/1min`
- 两台机器共享同一个 MySQL，即可并行铺底。

## 本地开发

```bash
pip install -r requirements.txt
cp config.example.yaml config.yaml  # 编辑填入MySQL连接信息
python main.py
```

## 配置

MySQL 连接：**环境变量 > config.yaml**

| 环境变量 | 说明 |
|---|---|
| `MYSQL_HOST` | MySQL地址 |
| `MYSQL_PORT` | MySQL端口 |
| `MYSQL_USER` | 用户名 |
| `MYSQL_PASSWORD` | 密码 |
| `MYSQL_DATABASE` | 数据库名 |

同步定时、请求间隔等参数见 `config.example.yaml`。

## 数据表

| 表名 | 说明 |
|---|---|
| `stock_info` | 沪深A股基础信息 |
| `kline_1min` | 1分钟K线（原始不复权），按季度分区 |
| `kline_5min` | 5分钟K线（原始不复权），按季度分区 |
| `xdxr_event` | 除权除息事件 |
| `sync_log` | 同步日志 |

## 技术栈

Python 3.9+ / FastAPI / pytdx + mootdx / MySQL / Docker (amd64 + arm64)
