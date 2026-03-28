
import asyncio
import os

import httpx

from main import REQUEST_TIMEOUT, build_alert_text, db, leader_fingerprint, scan_once, send_telegram_message, ALERT_COOLDOWN_MINUTES, DATABASE_URL


async def main() -> None:
    if DATABASE_URL:
        await db.connect()

    payload = await scan_once(save=True)
    candidates = []
    skipped = []

    for row in payload["alert_candidates"]:
        fingerprint = leader_fingerprint(row)
        if await db.recently_alerted(fingerprint, ALERT_COOLDOWN_MINUTES) if DATABASE_URL else False:
            skipped.append((row["symbol"], "cooldown"))
            continue
        candidates.append(row)

    text = build_alert_text(candidates)
    if not text:
        print("No fresh alert candidates.")
        if skipped:
            print("Skipped:", skipped)
        return

    async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
        ok, telegram_payload = await send_telegram_message(client, text)

    print("Sent:", ok)
    print("Symbols:", [x["symbol"] for x in candidates])
    print("Telegram:", telegram_payload)

    if ok and DATABASE_URL:
        for row in candidates:
            await db.mark_alert_sent(row["symbol"], leader_fingerprint(row))

    if DATABASE_URL:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
