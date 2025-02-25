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
import psycopg.rows
from kafka import KafkaProducer
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
from telethon.sessions import StringSession
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


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    else:
        return repr(value)


def _iceberg_json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return repr(value)


def iceberg_json_dumps(d: dict):
    return json.dumps(d, default=_iceberg_json_default).encode("utf-8")


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


def get_new_link_stats(prev_stats, update_stats):
    if prev_stats is None:
        new_stats = update_stats
    else:
        new_stats = {
            "nr_messages": prev_stats.get("nr_messages", 0)
            + update_stats["nr_messages"],
            "first_message_date": prev_stats.get(
                "first_message_date", update_stats["first_message_date"]
            ),
            "last_message_date": update_stats["last_message_date"],
        }
    return new_stats


def handle_linked_chan(
    channel_id,
    linked_username,
    link_stats,
    client,
    connection,
    pred_dist_from_core,
    producer,
    lang_priorities,
):
    with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT nr_messages, first_message_date, last_message_date"
            " FROM telegram.message_url_links"
            f" WHERE linking_channel_id = {channel_id}"
            f" AND linked_channel_username = '{linked_username}'"
        )
        prev_stats = cur.fetchone()

    new_stats = get_new_link_stats(prev_stats, link_stats)
    links_table_update_d = {
        "linking_channel_id": channel_id,
        "linked_channel_username": linked_username,
        **new_stats,
    }
    if prev_stats is None:
        collegram.utils.insert_into_postgres(
            connection, "telegram.message_url_links", links_table_update_d
        )
    else:
        collegram.utils.update_postgres(
            connection,
            "telegram.message_url_links",
            links_table_update_d,
            ["linking_channel_id", "linked_channel_username"],
        )

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
        " FROM telegram.channels_to_query"
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
                connection, "telegram.channels_to_query", insert_d
            )
            producer.send("telegram.chans_to_query", value=insert_d)

    if exists:
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
        new_nr_linking_channels = nr_linking_channels + int(prev_stats is None)
        update_d = {
            "id": channel_id,
            "nr_linking_channels": new_nr_linking_channels,
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
                new_nr_linking_channels,
                lang_priorities,
                acty_slope=5,
            )
            update_d["collection_priority"] = priority

        collegram.utils.update_postgres(
            connection, "telegram.channels_to_query", update_d, "id"
        )


def handle_forward(
    channel_id,
    fwd_id,
    fwd_stats,
    client,
    connection,
    pred_dist_from_core,
    producer,
    lang_priorities,
):
    with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT nr_messages, first_message_date, last_message_date"
            " FROM telegram.message_forward_links"
            f" WHERE linking_channel_id = {channel_id} AND linked_channel_id = {fwd_id}"
        )
        prev_stats = cur.fetchone()

    new_stats = get_new_link_stats(prev_stats, fwd_stats)
    fwds_table_update_d = {
        "linking_channel_id": channel_id,
        "linked_channel_id": fwd_id,
        **new_stats,
    }
    if prev_stats is None:
        collegram.utils.insert_into_postgres(
            connection, "telegram.message_forward_links", fwds_table_update_d
        )
    else:
        collegram.utils.update_postgres(
            connection,
            "telegram.message_forward_links",
            fwds_table_update_d,
            ["linking_channel_id", "linked_channel_id"],
        )

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
            " FROM telegram.channels_to_query"
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
        collegram.utils.insert_into_postgres(
            connection, "telegram.channels_to_query", insert_d
        )
        producer.send("telegram.chans_to_query", value=insert_d)

    else:
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
        new_nr_forwarding_channels = nr_forwarding_channels + int(prev_stats is None)
        update_d = {
            "id": fwd_id,
            "nr_forwarding_channels": new_nr_forwarding_channels,
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
                new_nr_forwarding_channels,
                nr_recommending_channels,
                nr_linking_channels,
                lang_priorities,
                acty_slope=5,
            )
            update_d["collection_priority"] = priority

        collegram.utils.update_postgres(
            connection, "telegram.channels_to_query", update_d, "id"
        )


async def collect_messages(
    client: TelegramClient,
    channel,
    dt_from: datetime.datetime,
    dt_to: datetime.datetime,
    forwards_stats: dict[int, dict],
    linked_chans_stats: dict[str, dict],
    anon_func,
    media_save_path: Path,
    fs,
    producer,
    query_id,
    offset_id=0,
):
    last_id = offset_id
    # Pass `fs` to following call to write embedded web pages as json artifacts
    async for m in collegram.messages.yield_channel_messages(
        client,
        channel,
        dt_from,
        dt_to,
        forwards_stats,
        linked_chans_stats,
        anon_func,
        media_save_path,
        offset_id=offset_id,
        fs=fs,
    ):
        m_dict = m.to_dict()
        m_dict["channel_id"] = channel.channel_id
        m_dict["query_id"] = query_id
        # MessageService have so many potential structures that putting them together
        # with normal messages in a table does not make sense.
        if isinstance(m, MessageService):
            producer.send(
                "telegram.raw_service_messages", value=iceberg_json_dumps(m_dict)
            )
        else:
            producer.send(
                "telegram.raw_messages", value=iceberg_json_dumps(m_dict)
            )
            m_dict = collegram.messages.flatten_dict(m, m_dict)
            producer.send(
                "telegram.messages", value=iceberg_json_dumps(m_dict)
            )
        last_id = m.id
    return last_id


def handler(context, event):
    nest_asyncio.apply()
    # Set relative priority for project's languages. Since the language detection is
    # surely not 100% reliable, have to allow for popular channels not detected as using
    # these to be collectable.
    lang_priorities = {
        lc: 1e-3 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]
    }

    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    only_top_priority = "ORDER BY collection_priority ASC LIMIT 1"
    cols = "id, access_hash, username, messages_last_queried_at, last_queried_message_id, distance_from_core"
    with connection.cursor() as cur:
        # First look for already-queried channel for which we need new messages
        cur.execute(f"SELECT id FROM telegram.channels_to_requery {only_top_priority}")
        chan_to_query = cur.fetchone()

    if chan_to_query is not None:
        (channel_id,) = chan_to_query
        with connection.cursor() as cur:
            cur.execute(
                f"SELECT {cols} FROM telegram.channels_to_query WHERE id = {channel_id}"
            )
            chan_to_query = cur.fetchone()
            try:
                cur.execute(
                    f"DELETE FROM telegram.channels_to_requery WHERE id = {channel_id}"
                )
                connection.commit()
            except Exception as e:
                print(e)
                cur.execute("ROLLBACK")
                connection.commit()

    else:
        # If there is no channel in the requerying queue, query a new one.
        with connection.cursor() as cur:
            cur.execute(
                f"SELECT {cols} FROM telegram.channels_to_query {only_top_priority}"
            )
            chan_to_query = cur.fetchone()

    if chan_to_query is None:
        return

    (channel_id, access_hash, channel_username, dt_from, last_queried_message_id, distance_from_core) = (
        chan_to_query
    )

    data_path = Path("/telegram/")
    paths = collegram.paths.ProjectPaths(data=data_path)
    media_save_path = paths.raw_data / "media"

    full_chat_d = collegram.channels.load(channel_id, paths, fs=fs)
    chat_d = collegram.channels.get_matching_chat_from_full(full_chat_d, channel_id)
    channel_username = chat_d.get("username")
    dt_from = dt_from or chat_d["date"]
    if isinstance(dt_from, str):
        dt_from = datetime.datetime.fromisoformat(dt_from)
    dt_from = dt_from.astimezone(datetime.timezone.utc)

    input_chat = get_input_chan(
        client,
        channel_username=channel_username,
        channel_id=channel_id,
        access_hash=access_hash,
    )
    context.logger.info(
        f"# Collecting messages from {channel_username} username, with ID {channel_id}"
    )

    def insert_anon_pair(original, anonymised):
        insert_d = {"original": original, "anonymised": anonymised}
        collegram.utils.insert_into_postgres(
            connection, table="telegram.anonymisation_map", values=insert_d
        )

    anonymiser = collegram.utils.HMAC_anonymiser(save_func=insert_anon_pair)

    global_dt_to = (
        datetime.datetime.now(datetime.timezone.utc)
        # - datetime.timedelta(days=30)
    )
    query_time = global_dt_to
    update_d = {"id": channel_id, "messages_last_queried_at": query_time}
    collegram.utils.update_postgres(
        connection, "telegram.channels_to_query", update_d, "id"
    )

    # Collect by chunks of a month to limit effects of a crash on the collection.
    dt_bin_edges = pl.datetime_range(
        dt_from, global_dt_to, interval="1mo", eager=True, time_zone="UTC"
    )

    forwarded_chans_stats = {}
    linked_chans_stats = {}

    for dt_from, dt_to in zip(dt_bin_edges[:-1], dt_bin_edges[1:]):
        chunk_fwds_stats = {}
        chunk_linked_chans_stats = {}

        query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
        query_info = {
            "query_id": str(uuid.uuid4()),
            "query_date": query_time,
            "data_owner": os.environ["TELEGRAM_OWNER"],
            "channel_id": input_chat.channel_id,
            "message_offset_id": last_queried_message_id,
        }

        context.logger.info(f"## Collecting messages from {dt_from} to {dt_to}")
        last_queried_message_id = client.loop.run_until_complete(
            collect_messages(
                client,
                input_chat,
                dt_from,
                dt_to,
                chunk_fwds_stats,
                chunk_linked_chans_stats,
                anonymiser.anonymise,
                media_save_path,
                fs,
                producer,
                query_info["query_id"],
                offset_id=last_queried_message_id,
            )
        )

        update_d = {"id": channel_id, "last_queried_message_id": last_queried_message_id}
        collegram.utils.update_postgres(
            connection, "telegram.channels_to_query", update_d, "id"
        )
        # Save metadata about the query itself
        m = json.loads(json.dumps(query_info, default=_json_default))
        producer.send("telegram.queries", value=m)

        for fwd_id, fwd_stats in chunk_fwds_stats.items():
            prev_stats = forwarded_chans_stats.get(fwd_id)
            end_chunk_stats = get_new_link_stats(prev_stats, fwd_stats)
            handle_forward(
                channel_id,
                fwd_id,
                end_chunk_stats,
                client,
                connection,
                distance_from_core,
                producer,
                lang_priorities,
            )
            forwarded_chans_stats[fwd_id] = end_chunk_stats

        for link_un, link_stats in chunk_linked_chans_stats.items():
            prev_stats = linked_chans_stats.get(link_un)
            end_chunk_stats = get_new_link_stats(prev_stats, link_stats)
            handle_linked_chan(
                channel_id,
                link_un,
                end_chunk_stats,
                client,
                connection,
                distance_from_core,
                producer,
                lang_priorities,
            )
            linked_chans_stats[link_un] = end_chunk_stats

    # Send a message to call this querier again.
    m = json.loads(json.dumps({"status": "chan_message_collection_done"}))
    producer.send("telegram.chans_to_message", m)
