# Korea Spot Sentiment Bot

一个可直接部署到 **GitHub + Railway** 的 Python 机器人。  
它会监控 **Upbit 韩国现货** 和 **Binance 合约价格**，并直接通过 **Telegram** 输出：

- `🟢 做多`
- `🔴 做空`

不再推送榜单，也不需要你看 Top10。

## 1. 功能说明

这个版本适合拿来做“韩国现货方向信号提示”：

- **数据源**
  - Upbit 公共 API
  - Binance Futures 公共 API
  - Telegram Bot API

- **信号逻辑**
  - `泡菜溢价 = (Upbit币价折算成USDT / Binance价格 - 1) * 100`
  - 折算汇率使用 **Upbit 的 KRW-USDT 价格**
  - 满足条件时只推送 `🟢 做多` 或 `🔴 做空`
  - 做多参考：溢价上升、Upbit领先、量能放大、短线动能增强
  - 做空参考：高溢价回落、Upbit领先不足、放量不涨、短线转弱

## 2. 项目结构

```bash
.
├── main.py
├── requirements.txt
├── Procfile
├── railway.json
└── .env.example
```

## 3. 本地运行

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python -X utf8 main.py
```

## 4. Telegram 获取 chat_id

最简单的方法：

1. 先给你的机器人发一条消息
2. 浏览器打开：

```bash
https://api.telegram.org/bot<你的TOKEN>/getUpdates
```

3. 找到里面的 `chat.id`
4. 填到 `.env` 或 Railway 变量里

## 5. Railway 部署

### 方法 A：GitHub 直接部署
1. 新建一个 GitHub 仓库
2. 把这些文件上传进去
3. Railway 里选择 **Deploy from GitHub Repo**
4. 添加环境变量（Variables）：

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SYMBOLS`
- `LOOP_SECONDS`
- `PREMIUM_THRESHOLD_PCT`
- `PREMIUM_CHANGE_THRESHOLD_PCT`
- `UPBIT_LEAD_THRESHOLD_PCT`
- `VOLUME_SPIKE_RATIO`
- `COOLDOWN_SECONDS`
- `CANDLE_COUNT`
- `LOG_LEVEL`

### 方法 B：本地推到 GitHub 后再连 Railway
```bash
git init
git add .
git commit -m "init korea signal bot"
git branch -M main
git remote add origin 你的仓库地址
git push -u origin main
```

然后在 Railway 连接这个仓库部署。

## 6. Railway 启动命令

这个项目已经带了 `Procfile`，一般不用额外改。  
Railway 会跑：

```bash
python -X utf8 main.py
```

## 7. 重要提醒

### 不是所有币都适合监控
这个机器人要求同一个币：
- 在 Upbit 有 `KRW-币种`
- 在 Binance Futures 有 `币种USDT`

例如：
- `BTCUSDT`
- `ETHUSDT`
- `XRPUSDT`
- `SOLUSDT`

某些很新的小币，如果其中一边没有市场，就不会产生信号。

### 这只是情绪提示，不是自动下单
它不会帮你自动开仓，只负责提醒。  
你可以后续再把它接到你自己的交易系统中。

## 8. 后续可升级方向

后面你可以继续加：

- Binance 成交额过滤
- 主力大单识别
- 假突破过滤
- Telegram 指令控制（比如 `/status`、`/symbols`）
- 数据落库
- 实时 WebSocket 版本
- 接你自己的固定币种白名单

## 9. 示例推送

```text
🟢 做多 | KITEUSDT
时间: 2026-03-30 21:30:00 北京时间
Upbit折算价: 0.345600
Binance价: 0.339100
泡菜溢价: 1.92%
溢价变化: +0.54%
量能比: 2.85x
短线动能: +1.11%
原因: 泡菜溢价 1.92% | 溢价上升 +0.54% | Upbit领先 1.92%
```

```text
🔴 做空 | SIGNUSDT
时间: 2026-03-30 21:36:00 北京时间
Upbit折算价: 0.812300
Binance价: 0.799000
泡菜溢价: 1.66%
溢价变化: -0.48%
量能比: 2.17x
短线动能: -0.31%
原因: 溢价回落 -0.48% | 短线转弱 -0.31% | 放量不涨
```
