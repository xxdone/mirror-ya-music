import asyncio
import logging
import random
import os
import re

from datetime import datetime
from dataclasses import dataclass
from get_hash import get_git_revision
from dotenv import load_dotenv
from telegram.ext import Application, ExtBot
from yandex_music import ClientAsync
import yandex_music

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

YANDEX_TOKEN = os.environ["YANDEX_TOKEN"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
PRIVATE_CHANNEL_ID = os.environ["PRIVATE_CHANNEL_ID"]

LIKED_TS: datetime | None = None
FILE_LIKED_TS = "liked_ts"

bitrates = [320, 192, 128, 64]
BATCH_TRACK_LIMIT = int(os.getenv("BATCH_TRACK_LIMIT", "5"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "5"))
MAX_CONCURRENT_UPLOADS = int(os.getenv("MAX_CONCURRENT_UPLOADS", "5"))
MAX_UPLOAD_RETRIES = int(os.getenv("MAX_UPLOAD_RETRIES", "6"))
UPLOAD_RETRY_BASE_DELAY_SECONDS = float(
    os.getenv("UPLOAD_RETRY_BASE_DELAY_SECONDS", "1.0")
)
UPLOAD_RETRY_MAX_DELAY_SECONDS = float(
    os.getenv("UPLOAD_RETRY_MAX_DELAY_SECONDS", "60.0")
)


def get_retry_after_seconds(error: Exception) -> float | None:
    # Telegram flood control errors typically contain: "Retry in N seconds"
    msg = str(error)
    retry_after = getattr(error, "retry_after", None)
    if retry_after is not None:
        try:
            return float(retry_after)
        except Exception:
            pass

    m = re.search(r"Retry in ([0-9]+(?:\.[0-9]+)?) seconds", msg)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    return None


@dataclass
class TrackPayload:
    timestamp: str
    performer: str
    title: str
    cover: bytes | None
    filename: str
    audio: bytes


def write_liked_ts(ts: str) -> None:
    global LIKED_TS
    LIKED_TS = datetime.fromisoformat(ts)
    with open(FILE_LIKED_TS, "w") as f:
        f.write(ts.strip())


async def get_latest_liked_ts() -> None:
    client = await yandex_music.ClientAsync(YANDEX_TOKEN).init()
    tracks = await client.users_likes_tracks()

    if tracks is None or len(tracks) == 0:
        raise Exception("Like at least one track")

    latest = tracks[0]
    write_liked_ts(latest.timestamp)


try:
    with open(FILE_LIKED_TS, "r") as f:
        ts = f.read()
        LIKED_TS = datetime.fromisoformat(ts)
except:
    logger.info(f"latest ts not found, getting new one")
    asyncio.run(get_latest_liked_ts())


async def prepare_track(track, download_sem: asyncio.Semaphore) -> TrackPayload:
    # Limit concurrent downloads to avoid hammering the upstream API.
    async with download_sem:
        track_info = await track.fetch_track_async()

        performer = ", ".join([artist.name for artist in track_info.artists])
        cover = None
        try:
            cover = await track_info.download_cover_bytes_async()
        except:
            logger.error("unable to download cover")

        track_bytes = None
        version = track_info.version and f" ({track_info.version})" or ""
        title = track_info.title + version

        for bitrate in bitrates:
            try:
                track_bytes = await track_info.download_bytes_async(
                    bitrate_in_kbps=bitrate
                )
                break
            except:
                pass

        if track_bytes is None:
            raise RuntimeError("track is unavailable")

        logger.info(f"downloading {title} - {performer}")

        return TrackPayload(
            timestamp=track.timestamp,
            performer=performer,
            title=title,
            cover=cover,
            filename=performer + " " + title,
            audio=track_bytes,
        )


async def start(client: ClientAsync, bot: ExtBot) -> None:

    tracks = await client.users_likes_tracks()

    new_tracks = []
    for track in tracks or []:
        if datetime.fromisoformat(track.timestamp) > LIKED_TS:  # type: ignore
            new_tracks.append(track)
        else:
            break

    new_tracks = sorted(new_tracks, key=lambda t: t.timestamp)

    if len(new_tracks):
        logger.info(f"latest ts {LIKED_TS}")
        total_new_tracks = len(new_tracks)
        new_tracks = new_tracks[:BATCH_TRACK_LIMIT]
        if total_new_tracks > BATCH_TRACK_LIMIT:
            logger.info(
                f"found {total_new_tracks} new tracks, processing first {len(new_tracks)}"
            )
        else:
            logger.info(f"found {len(new_tracks)} new tracks")

    if not new_tracks:
        return

    batch_tracks = list(new_tracks)
    batch_timestamps = [t.timestamp for t in batch_tracks]

    # 1) Concurrent download phase (bounded by MAX_CONCURRENT_DOWNLOADS)
    download_sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    prep_tasks = [
        asyncio.create_task(prepare_track(track, download_sem))
        for track in batch_tracks
    ]
    prep_results = await asyncio.gather(*prep_tasks, return_exceptions=True)

    payloads: list[TrackPayload | None] = [None] * len(batch_tracks)
    for i, res in enumerate(prep_results):
        if isinstance(res, Exception):
            logger.error(f"failed to prepare track: {res}")
            continue
        payloads[i] = res

    # 2) Concurrent upload phase (bounded by MAX_CONCURRENT_UPLOADS)
    upload_sem = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    uploaded_ok = [False] * len(batch_tracks)
    advance_idx = 0
    advance_lock = asyncio.Lock()

    async def upload_one(i: int, payload: TrackPayload) -> None:
        nonlocal advance_idx
        last_error: Exception | None = None
        for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
            try:
                async with upload_sem:
                    logger.info(f"uploading {payload.title} - {payload.performer}")
                    await bot.send_audio(
                        performer=payload.performer,
                        title=payload.title,
                        thumbnail=payload.cover,
                        filename=payload.filename,
                        chat_id=PRIVATE_CHANNEL_ID,
                        audio=payload.audio,
                    )

                async with advance_lock:
                    uploaded_ok[i] = True
                    while (
                        advance_idx < len(batch_tracks)
                        and uploaded_ok[advance_idx]
                    ):
                        write_liked_ts(batch_timestamps[advance_idx])
                        advance_idx += 1
                return
            except Exception as e:
                last_error = e
                msg = str(e)
                retryable = (
                    "Too Many Requests" in msg
                    or "Flood control exceeded" in msg
                    or "429" in msg
                )

                if not retryable or attempt >= MAX_UPLOAD_RETRIES:
                    break

                retry_after = get_retry_after_seconds(e)
                if retry_after is None:
                    # Exponential backoff fallback.
                    retry_after = (
                        UPLOAD_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                    )
                retry_after = min(retry_after, UPLOAD_RETRY_MAX_DELAY_SECONDS)
                # Small jitter to avoid thundering herd when many uploads fail together.
                retry_after += random.uniform(0, 0.5)

                logger.error(
                    f"failed to upload {payload.title}: {e} (retrying in {retry_after:.2f}s, attempt {attempt}/{MAX_UPLOAD_RETRIES})"
                )
                await asyncio.sleep(retry_after)

        if last_error is not None:
            logger.error(f"failed to upload {payload.title}: {last_error}")

    upload_tasks = [
        asyncio.create_task(upload_one(i, payload))
        for i, payload in enumerate(payloads)
        if payload is not None
    ]
    if upload_tasks:
        await asyncio.gather(*upload_tasks, return_exceptions=True)


async def loop(bot: ExtBot) -> None:
    client = await yandex_music.ClientAsync(YANDEX_TOKEN).init()
    logger.info(f"latest ts {LIKED_TS}")

    while True:
        await asyncio.sleep(1)
        try:
            await start(client, bot)
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")


def main() -> None:
    logger.info(f"Git hash: {get_git_revision('.')}")
    application = Application.builder().token(BOT_TOKEN).build()

    async def fire():
        await loop(application.bot)

    asyncio.run(fire())

    application.run_polling()


if __name__ == "__main__":
    main()
