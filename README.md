# 资金轮动监控器

基于 FastAPI + PostgreSQL 的多交易所资金轮动扫描服务。

## 功能

- 多交易所扫描：Binance / Bybit / OKX / Gate / MEXC / Upbit
- 历史快照写入 PostgreSQL
- 单币历史查询
- K线风格历史聚合
- 历史排行榜
- Telegram 测试消息
- Telegram 交易信号推送
- 自动后台扫描

## 目录结构

```text
.
├─ main.py
├─ requirements.txt
├─ Dockerfile
├─ docker-compose.yml
├─ .dockerignore
├─ .env.example
└─ README.md
