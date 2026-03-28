import asyncio

import httpx

from main import (
    ALERT_COOLDOWN_MINUTES,
    DATABASE_URL,
    REQUEST_TIMEOUT,
    build_alert_text,
    db,
    leader_fingerprint,
    scan_once,
    send_telegram_message,
)


async def main() -> None:
    if DATABASE_URL:
        ok = await db.connect()
        if not ok:
            print(f"数据库不可用，继续无库运行。错误={db.last_error}")

    payload = await scan_once(save=True)
    candidates = []
    skipped = []

    for row in payload["alert_candidates"]:
        fingerprint = leader_fingerprint(row)

        if db.pool:
            try:
                if await db.recently_alerted(fingerprint, ALERT_COOLDOWN_MINUTES):
                    skipped.append((row["symbol"], "冷却中"))
                    continue
            except Exception as e:
                db.last_error = f"{type(e).__name__}: {e}"
                print(f"冷却检测失败: {db.last_error}")

        candidates.append(row)

    text = build_alert_text(candidates)
    if not text:
        print("当前没有新的中文交易信号。")
        if skipped:
            print("跳过：", skipped)
        await db.close()
        return

    async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
        ok, telegram_payload = await send_telegram_message(client, text)

    print("发送结果：", ok)
    print("信号列表：", [(x["symbol"], x.get("交易信号")) for x in candidates])
    print("Telegram 响应：", telegram_payload)

    if ok and db.pool:
        for row in candidates:
            try:
                await db.mark_alert_sent(row["symbol"], leader_fingerprint(row))
            except Exception as e:
                db.last_error = f"{type(e).__name__}: {e}"
                print(f"告警记录写入失败: {db.last_error}")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
