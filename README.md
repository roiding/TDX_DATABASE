# TDX Database

A股分钟级K线数据采集入库工具。从通达信行情服务器拉取 **1分钟** 和 **5分钟** 原始K线数据，存入MySQL，解决通达信本地数据保留期限问题（1分钟仅100天，5分钟仅700天）。

## 核心设计

- **只存原始不复权数据** — 除权除息事件独立存表，前复权/后复权由应用层按需计算，避免每次除权导致历史数据变动
- **全量铺底 + 每日增量** — 首次拉取服务器全部可用历史，之后每天追加新数据
- **断点续传** — 全量同步中断后重新执行会跳过已同步的股票
- **幂等写入** — 重复执行不产生重复数据（INSERT IGNORE + ON DUPLICATE KEY UPDATE）
- **按季度分区** — K线表按时间分区，兼顾查询效率和数据管理

## 数据表

| 表名 | 说明 |
|---|---|
| `stock_info` | 沪深A股基础信息 |
| `kline_1min` | 1分钟K线（原始不复权），按季度分区 |
| `kline_5min` | 5分钟K线（原始不复权），按季度分区 |
| `xdxr_event` | 除权除息事件（分红、送股、配股等原始字段） |
| `sync_log` | 同步日志 |

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 准备配置（二选一）
# 方式A: 配置文件
cp config.example.yaml config.yaml
# 编辑 config.yaml 填入 MySQL 连接信息

# 方式B: 环境变量（Docker 部署推荐）
export MYSQL_HOST=xxx MYSQL_PORT=3306 MYSQL_USER=root MYSQL_PASSWORD=xxx MYSQL_DATABASE=tdx_data

# 云端 MySQL 建库
# CREATE DATABASE tdx_data CHARACTER SET utf8mb4;

# 初始化表结构
python main.py init

# 全量铺底（首次，约1-2小时）
python main.py full

# 每日增量（收盘后执行）
python main.py daily

# 查看同步状态
python main.py status

# 查询数据（调试）
python main.py query 000001 -f 5min -n 10
```

## Docker 部署

```bash
# 全量铺底
docker compose run tdx-sync full

# 每日增量
docker compose up
```

MySQL 连接信息在 `docker-compose.yaml` 的 `environment` 中配置。

## 配置优先级

MySQL 连接信息：**环境变量 > config.yaml**

| 环境变量 | 对应配置 |
|---|---|
| `MYSQL_HOST` | mysql.host |
| `MYSQL_PORT` | mysql.port |
| `MYSQL_USER` | mysql.user |
| `MYSQL_PASSWORD` | mysql.password |
| `MYSQL_DATABASE` | mysql.database |

## 技术栈

- **数据源**: pytdx（通达信行情服务器协议）
- **数据库**: MySQL（云端），按季度分区
- **语言**: Python 3.9+
