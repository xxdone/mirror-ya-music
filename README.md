# Mirror yandex music

<p align="center">
    <img src="https://i.imgur.com/2tNmw2W.png" alt="Mirror yandex music" width="300">
</p>

Simple python bot which runs in background and forwards liked :heart: tracks (mp3) from yandex music to your channel (might be private).

## Before you start

1. Get yandex token [With login and password](https://github.com/MarshalX/yandex-music-api/discussions/513#discussioncomment-11088072)
2. Get token of your channel (might be private)
3. Get token of your bot
4. Edit `.env.example` and rename it to `.env`
5. Create empty `liked_ts` file in root folder

## Install dependencies

`pip install -r requirements.txt`

## Run bot

`python3 post.py`

## Docker compose

`docker-compose up -d`


