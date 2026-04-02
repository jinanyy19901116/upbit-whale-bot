import asyncio
import json
import uuid
import time
import logging
from collections import defaultdict, deque

# ================= 5分钟周期 & 价格影响型参数 =================
# 20个核心非主流币监控
SYMBOLS = [
    "SOLUSDT", "SEIUSDT", "SUIUSDT", "APTUSDT", "NEARUSDT", 
    "STXUSDT", "ARBUSDT", "OPUSDT", "LINKUSDT", "PEPEUSDT",
    "WIFUSDT", "ONDOUSDT", "GRTUSDT", "RENDERUSDT", "MINAUSDT",
    "THETAUSDT", "EGLDUSDT", "SHIBUSDT", "DOGEUSDT", "METISUSDT"
]

# 影响价格的起步门槛 (5分钟累计)
# 主流非主流 (如SOL): 100万刀 | 普通非主流: 40万刀
IMPACT_THRESHOLD = 400000 
UPBIT_IMPACT_KRW = 500000000 # 5亿韩元 (约37万美金) 起报
# =========================================================

logging.basicConfig(level=logging.INFO, format='%(message)s')

class PriceImpactMonitor:
    def __init__(self):
        # 存储5分钟内的交易数据: {symbol: deque([{'amt': 100, 'side': 'buy', 'ts': 123}, ...])}
        self.flow_buffer = defaultdict(lambda: deque())
        self.last_alert_ts = {}

    def get_net_flow(self, symbol):
        """ 计算5分钟内的净资金流 """
        now = time.time()
        # 清理过期数据
        while self.flow_buffer[symbol] and now - self.flow_buffer[symbol][0]['ts'] > 300:
            self.flow_buffer[symbol].popleft()
        
        buys = sum(t['amt'] for t in self.flow_buffer[symbol] if t['side'] == 'buy')
        sells = sum(t['amt'] for t in self.flow_buffer[symbol] if t['side'] == 'sell')
        return buys, sells, (buys - sells)

    async def binance_impact_ws(self):
        import websockets
        streams = "/".join([f"{s.lower()}@aggTrade" for s in SYMBOLS])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        async with websockets.connect(url) as ws:
            logging.info("📡 币安 5min 价格影响监控已就绪...")
            while True:
                res = json.loads(await ws.recv())['data']
                s, p, q = res['s'], float(res['p']), float(res['q'])
                side = "sell" if res['m'] else "buy"
                self.flow_buffer[s].append({'amt': p * q, 'side': side, 'ts': time.time()})
                
                # 每隔10秒检查一次是否触发阈值
                if int(time.time()) % 10 == 0:
                    b, s_vol, net = self.get_net_flow(s)
                    abs_net = abs(net)
                    # 动态门槛：SOL/DOGE提高门槛
                    dynamic_limit = IMPACT_THRESHOLD * 2 if s in ["SOLUSDT", "DOGEUSDT"] else IMPACT_THRESHOLD
                    
                    if abs_net >= dynamic_limit:
                        self.trigger_alert(s, net, "Binance Futures")

    async def upbit_impact_ws(self):
        import websockets
        url = "wss://api.upbit.com/websocket/v1"
        codes = [f"KRW-{s.replace('USDT', '')}".replace("RENDER", "RNDR") for s in SYMBOLS]
        async with websockets.connect(url) as ws:
            sub_msg = [{"ticket": str(uuid.uuid4())}, {"type": "trade", "codes": codes}]
            await ws.send(json.dumps(sub_msg))
            logging.info("🇰🇷 Upbit 价格引爆点监控已就绪...")
            while True:
                data = json.loads(await ws.recv())
                s, p, v = data['code'], data['trade_price'], data['trade_volume']
                side = "buy" if data['ask_bid'] == "BID" else "sell"
                # Upbit 数据直接触发，因为韩元单笔大单极具代表性
                amt_usd = (p * v) / 1350
                if (p * v) >= UPBIT_IMPACT_KRW:
                    self.trigger_alert(s, amt_usd if side == "buy" else -amt_usd, "Upbit Spot")

    def trigger_alert(self, symbol, net_flow, source):
        # 3分钟冷却，防止震荡行情刷屏
        if time.time() - self.last_alert_ts.get(symbol, 0) < 180: return
        
        direction = "🚀 强力多头注入" if net_flow > 0 else "💀 恐慌性抛售"
        logging.info(f"\n级别: 5分钟价格影响单\n源头: {source}\n标的: {symbol}\n方向: {direction}\n净值: ${abs(net_flow)/10000:.1f}万\n状态: 1H趋势可能反转/加速\n" + "-"*30)
        self.last_alert_ts[symbol] = time.time()

    async def main(self):
        await asyncio.gather(self.binance_impact_ws(), self.upbit_impact_ws())

if __name__ == "__main__":
    monitor = PriceImpactMonitor()
    asyncio.run(monitor.main())
