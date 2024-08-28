import datetime
import json
import os
from pathlib import Path

import collegram
import fsspec
import nest_asyncio
import polars as pl
import psycopg
from kafka import KafkaProducer
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
from telethon.sessions import StringSession


async def init_context(context):
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret = os.environ["MINIO_SECRET_KEY"]
    minio_home = os.environ["MINIO_HOME"]
    storage_options = {
        "endpoint_url": f"https://{minio_home}",
        "key": access_key,
        "secret": secret,
    }
    fs = fsspec.filesystem("s3", **storage_options)
    setattr(context, "fs", fs)

    # Connect to an existing database
    connection = psycopg.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PW"],
        host=os.environ["POSTGRES_IP"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
    )
    setattr(context, "connection", connection)

    # prefix = os.environ["TELEGRAM_OWNER"].upper()
    client = TelegramClient(
        StringSession(os.environ["AI4TRUST_TG_SESSION"]),
        os.environ["AI4TRUST_API_ID"],
        os.environ["AI4TRUST_API_HASH"],
        flood_sleep_threshold=24 * 3600,
    )
    setattr(context, "client", client)

    broker = os.environ.get("KAFKA_BROKER")
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def handle_new_forward(fwd_id, client, connection, pred_dist_from_core):
    with connection.cursor() as cur:
        cur.execute(
            f"SELECT nr_forwarding_channels, distance_from_core FROM channels_to_query WHERE id = {fwd_id}"
        )
        prio_info = cur.fetchone()

    if prio_info is None:
        try:
            fwd_input_peer_channel = collegram.channels.get_input_peer(
                client, channel_id=fwd_id
            )
        except ChannelPrivateError:
            # These channels are valid and have been seen for sure,
            # might be private though.
            return
        except (ChannelInvalidError, ValueError):
            # This should happen extremely rarely, still haven't figured
            # out conditions under which it does.
            return

        fwd_hash = fwd_input_peer_channel.access_hash
        insert_d = {
            "id": fwd_id,
            "access_hash": fwd_hash,
            "nr_forwarding_channels": 1,
            "distance_from_core": pred_dist_from_core + 1,
        }
        collegram.utils.insert_into_postgres(connection, "channels_to_query", insert_d)

    else:
        (nr_forwarding, distance_from_core) = prio_info
        update_d = {
            "id": fwd_id,
            "nr_forwarding_channels": nr_forwarding + 1,
            "distance_from_core": min(pred_dist_from_core + 1, distance_from_core),
            # TODO
            # "priority": 0,
        }
        collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")


def handler(context, event):
    nest_asyncio.apply()

    # TODO: handle passing forwards to "previous" component + anon vs non anon ID in data
    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    data = json.loads(event.body.decode("utf-8"))
    channel_id = data["channel_id"]
    access_hash = data["access_hash"]
    channel_username = data.get("channel_username")
    dist_from_core = data["distance_from_core"]

    data_path = Path("/telegram/")
    paths = collegram.paths.ProjectPaths(data=data_path)
    media_save_path = paths.raw_data / "media"
    chan_paths = collegram.paths.ChannelPaths(channel_id, paths)

    full_chat_d = collegram.channels.load(channel_id, paths, fs=fs)
    chat_d = collegram.channels.get_matching_chat_from_full(full_chat_d, channel_id)
    # TODO: following "dt_from" should be set by orchestrator, taking into account `messages_last_queried_at`
    dt_from = datetime.datetime.fromisoformat(data.get("dt_from", chat_d["date"]))

    try:
        input_chat = collegram.channels.get_input_peer(
            client,
            channel_username=channel_username,
            channel_id=channel_id,
            access_hash=access_hash,
        )
    except (
        ChannelInvalidError,
        ChannelPrivateError,
        UsernameInvalidError,
        ValueError,
    ) as e:
        # TODO What then?
        # For all but ChannelPrivateError, can try with another key (TODO: add to
        # list of new channels?).
        # logger.warning(f"could not get data for listed channel {channel_id}")
        raise e

    def insert_anon_pair(original, anonymised):
        insert_d = {"original": original, "anonymised": anonymised}
        collegram.utils.insert_into_postgres(
            connection, table="anonymisation_map", values=insert_d
        )

    anonymiser = collegram.utils.HMAC_anonymiser(save_func=insert_anon_pair)

    global_dt_to = (
        datetime.datetime.now(datetime.timezone.utc)
        # - datetime.timedelta(days=30)
    )
    query_time = global_dt_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    update_d = {"id": channel_id, "messages_last_queried_at": query_time}
    collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")

    dt_bin_edges = pl.datetime_range(
        dt_from, global_dt_to, interval="1mo", eager=True, time_zone="UTC"
    )

    forwarded_chans = set()
    # Caution: the sorting only works because of file name format!
    existing_files = sorted(list(fs.ls(chan_paths.messages)))

    for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
        chunk_fwds = set()
        dt_from_in_path = dt_from.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ).date()
        messages_save_path = (
            chan_paths.messages / f"{dt_from_in_path}_to_{dt_to.date()}.jsonl"
        )
        is_last_saved_period = (
            len(existing_files) > 0 and messages_save_path == existing_files[-1]
        )

        if not fs.exists(messages_save_path) or is_last_saved_period:
            offset_id = 0
            if is_last_saved_period:
                # Get the offset in case collection was unexpectedly interrupted
                # while writing for this time range.
                last_message_saved = collegram.utils.read_nth_to_last_line(
                    messages_save_path,
                    fs=fs,
                )
                # Check if not empty file before reading message
                if last_message_saved:
                    offset_id = collegram.json.read_message(last_message_saved).id

            # Save messages, don't get to avoid overflowing memory.
            client.loop.run_until_complete(
                collegram.messages.save_channel_messages(
                    client,
                    input_chat,
                    dt_from,
                    dt_to,
                    chunk_fwds,
                    anonymiser.anonymise,
                    messages_save_path,
                    media_save_path,
                    offset_id=offset_id,
                    fs=fs,
                )
            )
            new_fwds = chunk_fwds.difference(forwarded_chans)

            for fwd_id in new_fwds:
                forwarded_chans.add(fwd_id)
                handle_new_forward(fwd_id, client, connection, dist_from_core)

    # TODO: say we're done
