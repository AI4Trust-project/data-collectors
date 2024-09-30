import datetime
import json
import os
import uuid
from pathlib import Path
from typing import Optional

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
from telethon.tl.tlobject import _json_default
from telethon.types import InputPeerChannel, MessageService


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
    await client.start(os.environ["AI4TRUST_PHONE_NUMBER"])
    setattr(context, "client", client)

    broker = os.environ.get("KAFKA_BROKER")
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda x: json.dumps(x, default=_json_default).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def get_input_chan(
    client,
    channel_username: Optional[str] = None,
    channel_id: Optional[int] = None,
    access_hash: Optional[int] = None,
):
    try:
        fwd_input_peer_channel = collegram.channels.get_input_peer(
            client, channel_username, channel_id, access_hash
        )
        return fwd_input_peer_channel
    except ChannelPrivateError:
        # These channels are valid and have been seen for sure,
        # might be private though. TODO: keep track of private channels!
        return
    except (ChannelInvalidError, UsernameInvalidError, ValueError):
        # This should happen extremely rarely, still haven't figured
        # out conditions under which it does.
        return


def handle_new_linked_chan(
    linked_username, client, connection, pred_dist_from_core, producer, lang_priorities
):
    base_query = (
        "SELECT"
        " id,"
        " created_at,"
        " channel_last_queried_at,"
        " language_code,"
        " nr_participants,"
        " nr_messages,"
        " nr_forwarding_channels,"
        " nr_recommending_channels,"
        " nr_linking_channels,"
        " distance_from_core"
        " FROM channels_to_query"
    )
    with connection.cursor() as cur:
        cur.execute(base_query + f" WHERE username = '{linked_username}'")
        prio_info = cur.fetchone()

    exists = prio_info is not None
    if not exists:
        input_peer_channel = get_input_chan(client, channel_username=linked_username)
        if not isinstance(input_peer_channel, InputPeerChannel):
            return

        # Here there is a possibility the username changed, so check again the existence
        # based on the ID.
        with connection.cursor() as cur:
            cur.execute(base_query + f" WHERE id = {input_peer_channel.channel_id}")
            prio_info = cur.fetchone()

        exists = prio_info is not None
        if not exists:
            insert_d = {
                "id": input_peer_channel.channel_id,
                "access_hash": input_peer_channel.access_hash,
                "username": linked_username,
                "data_owner": os.environ["TELEGRAM_OWNER"],
                "nr_linking_channels": 1,
                "distance_from_core": pred_dist_from_core + 1,
            }
            collegram.utils.insert_into_postgres(
                connection, "channels_to_query", insert_d
            )
            producer.send("chans_to_query", value=insert_d)

    if exists:
        # TODO: handle case in which previous message collection already found this
        # channel
        (
            channel_id,
            created_at,
            channel_last_queried_at,
            language_code,
            participants_count,
            messages_count,
            nr_forwarding_channels,
            nr_recommending_channels,
            nr_linking_channels,
            distance_from_core,
        ) = prio_info
        new_dist_from_core = min(pred_dist_from_core + 1, distance_from_core)
        update_d = {
            "id": channel_id,
            "nr_linking_channels": nr_linking_channels + 1,
            "distance_from_core": new_dist_from_core,
        }

        # If channel has already been queried by `chan-querier`, then recompute
        # priority.
        if channel_last_queried_at is not None:
            lifespan_seconds = (created_at - channel_last_queried_at).total_seconds()
            priority = collegram.channels.get_explo_priority(
                language_code,
                messages_count,
                participants_count,
                lifespan_seconds,
                new_dist_from_core,
                nr_forwarding_channels,
                nr_recommending_channels,
                nr_linking_channels + 1,
                lang_priorities,
                acty_slope=5,
            )
            update_d["collection_priority"] = priority

        collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")


def handle_new_forward(
    fwd_id, client, connection, pred_dist_from_core, producer, lang_priorities
):
    with connection.cursor() as cur:
        cur.execute(
            "SELECT"
            " created_at,"
            " channel_last_queried_at,"
            " language_code,"
            " nr_participants,"
            " nr_messages,"
            " nr_forwarding_channels,"
            " nr_recommending_channels,"
            " nr_linking_channels,"
            " distance_from_core"
            " FROM channels_to_query"
            f" WHERE id = {fwd_id}"
        )
        prio_info = cur.fetchone()

    if prio_info is None:
        fwd_input_peer_channel = get_input_chan(client, channel_id=fwd_id)
        if fwd_input_peer_channel is None:
            return
        fwd_hash = fwd_input_peer_channel.access_hash
        insert_d = {
            "id": fwd_id,
            "access_hash": fwd_hash,
            "data_owner": os.environ["TELEGRAM_OWNER"],
            "nr_forwarding_channels": 1,
            "distance_from_core": pred_dist_from_core + 1,
        }
        collegram.utils.insert_into_postgres(connection, "channels_to_query", insert_d)
        producer.send("chans_to_query", value=insert_d)

    else:
        # TODO: handle case in which previous message collection already found this
        # channel
        (
            created_at,
            channel_last_queried_at,
            language_code,
            participants_count,
            messages_count,
            nr_forwarding_channels,
            nr_recommending_channels,
            nr_linking_channels,
            distance_from_core,
        ) = prio_info
        new_dist_from_core = min(pred_dist_from_core + 1, distance_from_core)
        update_d = {
            "id": fwd_id,
            "nr_forwarding_channels": nr_forwarding_channels + 1,
            "distance_from_core": new_dist_from_core,
        }

        # If channel has already been queried by `chan-querier`, then recompute
        # priority.
        if channel_last_queried_at is not None:
            lifespan_seconds = (created_at - channel_last_queried_at).total_seconds()
            priority = collegram.channels.get_explo_priority(
                language_code,
                messages_count,
                participants_count,
                lifespan_seconds,
                new_dist_from_core,
                nr_forwarding_channels + 1,
                nr_recommending_channels,
                nr_linking_channels,
                lang_priorities,
                acty_slope=5,
            )
            update_d["collection_priority"] = priority

        collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")


async def collect_messages(
    client: TelegramClient,
    channel,
    dt_from: datetime.datetime,
    dt_to: datetime.datetime,
    forwards_set: set[int],
    linked_chans: set[str],
    anon_func,
    messages_save_path: Path,
    media_save_path: Path,
    fs,
    producer,
    query_id,
    offset_id=0,
):
    with fs.open(messages_save_path, "a") as f:
        async for m in collegram.messages.yield_channel_messages(
            client,
            channel,
            dt_from,
            dt_to,
            forwards_set,
            linked_chans,
            anon_func,
            media_save_path,
            offset_id=offset_id,
            fs=fs,
        ):
            m_json = m.to_json()
            f.write(m_json)
            f.write("\n")

            # MessageService have so many potential structures that putting them in a
            # table does not make sense.
            if not isinstance(m, MessageService):
                m_dict = collegram.messages.to_flat_dict(m)
                m_dict["table"] = "telegram-channel-messages"
                m_dict["channel_id"] = channel.channel_id
                m_dict["query_id"] = query_id
                # send message to iceberg
                producer.send("telegram_collected_messages", value=m_dict)


def handler(context, event):
    nest_asyncio.apply()
    # Set relative priority for project's languages. Since the language detection is
    # surely not 100% reliable, have to allow for popular channels not detected as using
    # these to be collectable.
    lang_priorities = {
        lc: 1e-3 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]
    }

    # TODO: handle passing forwards to "previous" component + anon vs non anon ID in data
    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    data = json.loads(event.body.decode("utf-8"))
    channel_id = data["channel_id"]
    access_hash = data["access_hash"]
    dist_from_core = data["distance_from_core"]

    data_path = Path("/telegram/")
    paths = collegram.paths.ProjectPaths(data=data_path)
    media_save_path = paths.raw_data / "media"
    chan_paths = collegram.paths.ChannelPaths(channel_id, paths)

    full_chat_d = collegram.channels.load(channel_id, paths, fs=fs)
    chat_d = collegram.channels.get_matching_chat_from_full(full_chat_d, channel_id)
    channel_username = chat_d.get("username")
    # TODO: following "dt_from" should be set by orchestrator, taking into account `messages_last_queried_at`
    dt_from = datetime.datetime.fromisoformat(data.get("dt_from", chat_d["date"]))

    input_chat = get_input_chan(
        client,
        channel_username=channel_username,
        channel_id=channel_id,
        access_hash=access_hash,
    )

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
    query_time = global_dt_to
    update_d = {"id": channel_id, "messages_last_queried_at": query_time}
    collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")

    dt_bin_edges = pl.datetime_range(
        dt_from, global_dt_to, interval="1mo", eager=True, time_zone="UTC"
    )

    forwarded_chans = set()
    linked_chans = {channel_username} if channel_username is not None else set()

    # Caution: the sorting only works because of file name format!
    existing_files = sorted(list(fs.glob(f"{chan_paths.messages}/*.jsonl")))

    for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
        chunk_fwds = set()
        chunk_linked_chans = set()
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

            query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
            query_info = {
                "query_id": str(uuid.uuid4()),
                "query_date": query_time,
                "data_owner": os.environ["TELEGRAM_OWNER"],
                "channel_id": input_chat.channel_id,
                "message_offset_id": offset_id,
                "result_path": str(messages_save_path.absolute()),
            }

            client.loop.run_until_complete(
                collect_messages(
                    client,
                    input_chat,
                    dt_from,
                    dt_to,
                    chunk_fwds,
                    chunk_linked_chans,
                    anonymiser.anonymise,
                    messages_save_path,
                    media_save_path,
                    fs,
                    producer,
                    query_info["query_id"],
                    offset_id=offset_id,
                )
            )

            # Save metadata about the query itself
            m = json.loads(json.dumps(query_info, default=_json_default))
            m["table"] = "telegram-queries"
            producer.send("telegram_collected_metadata", value=m)

            new_fwds = chunk_fwds.difference(forwarded_chans)
            for fwd_id in new_fwds:
                forwarded_chans.add(fwd_id)
                handle_new_forward(
                    fwd_id,
                    client,
                    connection,
                    dist_from_core,
                    producer,
                    lang_priorities,
                )

            new_linked_chans = chunk_linked_chans.difference(linked_chans)
            for linked_un in new_linked_chans:
                linked_chans.add(linked_un)
                handle_new_linked_chan(
                    linked_un,
                    client,
                    connection,
                    dist_from_core,
                    producer,
                    lang_priorities,
                )

    # TODO: say we're done
