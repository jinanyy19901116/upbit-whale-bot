# Watchlist Rotation Scanner v3

这版是可实战一点的版本，已经把你要的三件事补上了：

- PostgreSQL 历史快照
- Telegram 自动推送
- 可拆成 Railway Web 服务 + Railway Cron Runner

## 支持的交易所

- Binance USDT Perp
- Bybit Linear Perp
- OKX USDT Swap
- Gate Spot
- MEXC Futures
- Upbit Spot

## 新增能力

### 1. 历史快照
每次扫描都可以把当前截面写入 Postgres：
- 24h 成交额
- 24h 涨跌幅
- OI / OI USD
- funding
- signal_score

### 2. 历史对比
会尝试拿 `SNAPSHOT_LOOKBACK_MINUTES` 之前的最近一条快照做比较，算出：
- `volume_growth_pct_vs_baseline`
- `oi_growth_pct_vs_baseline`
- `signal_growth_vs_baseline`

### 3. Telegram 自动推送
不只是看当前分数，还会要求满足至少一个“正在变强”的条件：
- signal 分数比 baseline 提升
- 成交额比 baseline 增长
- OI 比 baseline 增长

### 4. 冷却时间
为了避免同一个币不停重复推送，增加了：
- `ALERT_COOLDOWN_MINUTES`

## 文件说明

- `main.py`：API 服务
- `runner.py`：给 Railway Cron 用的扫描发送器

## API

### `GET /scan`
完整扫描，默认会存库

### `GET /scan?symbol=SIRENUSDT`
看单个币在不同交易所的实时状态

### `GET /alerts/preview`
先预览哪些币会被推送

### `GET /alerts/send`
立即执行一次推送

### `GET /history/status`
查看数据库里是否已经有快照

## Railway 推荐部署方式

建议你建 3 个服务：

### A. API 服务
- 从 GitHub 部署这个项目
- Start command:
```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

### B. PostgreSQL 服务
- 直接加 Railway Postgres
- 把生成的 `DATABASE_URL` 共享给 API 和 Cron

### C. Cron Runner
- 同一个仓库再建一个 service
- Start command:
```bash
python runner.py
```
- 然后把这个 service 配成 cron job，例如：
```cron
*/5 * * * *
```

## 变量

```env
WATCHLIST=...
UPBIT_REGION=sg
UPBIT_QUOTE=USDT
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
DATABASE_URL=
MIN_SIGNAL_TO_ALERT=55
MIN_EXCHANGES_TO_ALERT=2
SNAPSHOT_LOOKBACK_MINUTES=60
ALERT_COOLDOWN_MINUTES=90
LEADER_LIMIT=50
```

## 本地运行

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# API
uvicorn main:app --reload

# 或跑一次发送器
python runner.py
```

## 推荐上线步骤

1. 先只开 API，不开 cron
2. 连上 Postgres，手动打几次 `/scan`
3. 确认 `history/status` 有数据
4. 再看 `/alerts/preview`
5. 最后再开 cron 跑 `runner.py`

## 后面还值得继续加的

- 5m / 15m K线级别成交额突增
- WebSocket 实时流
- 现货领先、合约跟随的联动规则
- 单独的“交易所领先因子”
- 更细的告警分组
