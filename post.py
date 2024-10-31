import asyncio
import logging
import os

from datetime import datetime
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


def write_liked_ts(ts: str) -> None:
    global LIKED_TS
    LIKED_TS = datetime.fromisoformat(ts)
    with open(FILE_LIKED_TS, "w") as f:
        f.write(ts)


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


async def start(client: ClientAsync, bot: ExtBot) -> None:

    tracks = await client.users_likes_tracks()

    new_tracks = []
    for track in tracks or []:
        if datetime.fromisoformat(track.timestamp) > LIKED_TS:  # type: ignore
            new_tracks.append(track)
        else:
            break

    # new_tracks.reverse()

    if len(new_tracks):
        logger.info(f"latest ts {LIKED_TS}")
        logger.info(f"found {len(new_tracks)} new tracks")

    for track in new_tracks:
        track_info = await track.fetch_track_async()

        performer = ", ".join([artist.name for artist in track_info.artists])
        cover = await track_info.download_cover_bytes_async()
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
            logger.error("track is unavailable")
            break

        logger.info(f"downloading {title} - {performer}")

        await bot.send_audio(
            performer=performer,
            title=title,
            thumbnail=cover,
            chat_id=PRIVATE_CHANNEL_ID,
            audio=track_bytes,
        )

        write_liked_ts(track.timestamp)


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
