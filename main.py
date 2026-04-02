import asyncio
import json
import time
import os
import logging
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from dotenv import load_dotenv

# 配置
load_dotenv()
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================= 策略参数（超短线合约适配） =================
BIG_ORDER_THRESHOLD = 100000  # 10万 USDT 起报
NET_RATIO_REQ = 3.5           # 买盘需是卖盘 3.5 倍以上
SQUEEZE_RANGE = 0.003         # 0.3% 以内的窄幅挤压
CONFIRM_WAIT = 5              # 5秒价格确认
MAX_SYMBOLS_PER_WS = 150      # 币安单个 WS 建议订阅上限
# ==========================================================

class UltimateWhaleBot:
    def __init__(self):
        self.symbols = []
        self.history = defaultdict(lambda: deque(maxlen=100))
        self.prices = defaultdict(lambda: deque(maxlen=60))
        self.last_alert_time = {}
        self.session = None
        self.tele_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

    async def get_active_symbols(self):
        """获取所有币安在线合约交易对"""
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        async with self.session.get(url) as r:
            data = await r.json()
            # 过滤只保留 USDT 合约，且状态为 TRADING
            return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['symbol'].endswith('USDT')]

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    def is_squeezing(self, symbol):
        """库拉马吉策略核心：检查是否有收缩形态"""
        p_list = list(self.prices[symbol])
        if len(p_list) < 30: return False
        amp = (max(p_list) - min(p_list)) / min(p_list)
        return amp <= SQUEEZE_RANGE

    def get_flow_stats(self, symbol):
        """计算资金合力"""
        now = time.time()
        trades = [t for t in self.history[symbol] if now - t['ts'] < 60]
        buys = sum(t['amt'] for t in trades if t['side'] == 'buy')
        sells = sum(t['amt'] for t in trades if t['side'] == 'sell')
        if sells == 0: return ("BUY", buys, 99) if buys > 0 else (None, 0, 0)
        return ("BUY" if buys/sells > NET_RATIO_REQ else None), (buys - sells), (buys/sells)

    async def send_tg(self, msg):
        if not self.tele_token: logging.info(msg); return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        try:
            await self.session.post(url, json={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"})
        except Exception as e: logging.error(f"TG Error: {e}")

    async def process_signal(self, symbol, entry_p):
        """库拉马吉确认逻辑"""
        # 1. 检查是否在窄幅震荡（挤压阶段）
        if not self.is_squeezing(symbol): return
        
        # 2. 冷却逻辑，防止同一个币刷屏
        if time.time() - self.last_alert_time.get(symbol, 0) < 300: return

        # 3. 价格确认期
        await asyncio.sleep(CONFIRM_WAIT)
        
        curr_p = list(self.prices[symbol])[-1] if self.prices[symbol] else entry_p
        side, net_flow, ratio = self.get_flow_stats(symbol)
        
        # 4. 最终判定：资金合力+价格站稳
        if side == "BUY" and curr_p > entry_p * 1.001:
            self.last_alert_time[symbol] = time.time()
            tp, sl = curr_p * 1.015, curr_p * 0.994 # 1.5% 止盈, 0.6% 止损
            
            text = (
                f"🌟 <b>【全网合约·库拉马吉突破】</b>\n"
                f"<b>标的:</b> #{symbol}\n"
                f"<b>资金:</b> 净流入 {net_flow/10000:.1f}万 ({ratio:.1f}倍)\n"
                f"<b>形态:</b> 窄幅收缩后放量突破 ✅\n"
                f"------------------------\n"
                f"<b>🎯 建议入场价:</b> {curr_p:.6f}\n"
                f"<b>💰 建议止盈:</b> {tp:.6f}\n"
                f"<b>🛡 建议止损:</b> {sl:.6f}\n"
                f"------------------------\n"
                f"提示：库拉马吉策略核心在于‘追强杀弱’，入场后跌破止损即离场。"
            )
            await self.send_tg(text)

    async def subscribe_ws(self, symbols_chunk):
        """订阅币安聚合成交流"""
        streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols_chunk])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        
        while True:
            try:
                async with websockets.connect(url) as ws:
                    logging.info(f"成功订阅 {len(symbols_chunk)} 个币种")
                    while True:
                        res = await ws.recv()
                        data = json.loads(res)['data']
                        symbol, p, q = data['s'], float(data['p']), float(data['q'])
                        amt = p * q
                        
                        self.prices[symbol].append(p)
                        side = "sell" if data['m'] else "buy"
                        self.history[symbol].append({'ts': time.time(), 'side': side, 'amt': amt})
                        
                        if amt >= BIG_ORDER_THRESHOLD and side == "buy":
                            asyncio.create_task(self.process_signal(symbol, p))
            except Exception as e:
                logging.error(f"WS连接异常: {e}, 5秒后重连...")
                await asyncio.sleep(5)

    async def main(self):
        await self.init_session()
        # 1. 获取全币种
        all_symbols = await self.get_active_symbols()
        logging.info(f"检测到 {len(all_symbols)} 个活跃合约对")
        
        # 2. 分块订阅（防止超过币安 WS 限制）
        chunks = [all_symbols[i:i + MAX_SYMBOLS_PER_WS] for i in range(0, len(all_symbols), MAX_SYMBOLS_PER_WS)]
        
        tasks = [self.subscribe_ws(chunk) for chunk in chunks]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    bot = UltimateWhaleBot()
    try:
        asyncio.run(bot.main())
    except KeyboardInterrupt:
        pass
