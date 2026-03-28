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
            print(f"Database unavailable, continue without Postgres. error={db.last_error}")

    payload = await scan_once(save=True)
    candidates = []
    skipped = []

    for row in payload["alert_candidates"]:
        fingerprint = leader_fingerprint(row)

        if db.pool:
            try:
                if await db.recently_alerted(fingerprint, ALERT_COOLDOWN_MINUTES):
                    skipped.append((row["symbol"], "cooldown"))
                    continue
            except Exception as e:
                db.last_error = f"{type(e).__name__}: {e}"
                print(f"recently_alerted failed: {db.last_error}")

        candidates.append(row)

    text = build_alert_text(candidates)
    if not text:
        print("No fresh alert candidates.")
        if skipped:
            print("Skipped:", skipped)
        await db.close()
        return

    async with httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT)) as client:
        ok, telegram_payload = await send_telegram_message(client, text)

    print("Sent:", ok)
    print("Symbols:", [x["symbol"] for x in candidates])
    print("Telegram:", telegram_payload)

    if ok and db.pool:
        for row in candidates:
            try:
                await db.mark_alert_sent(row["symbol"], leader_fingerprint(row))
            except Exception as e:
                db.last_error = f"{type(e).__name__}: {e}"
                print(f"mark_alert_sent failed: {db.last_error}")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
