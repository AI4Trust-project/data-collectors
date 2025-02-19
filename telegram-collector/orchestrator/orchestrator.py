import datetime
import os

import collegram
import psycopg
import psycopg.rows


def init_context(context):
    connection = psycopg.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PW"],
        host=os.environ["POSTGRES_IP"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
    )
    setattr(context, "connection", connection)


def handler(context, event):
    # Periodic part of collection triggered by cron job, to get new messages from
    # already-queried channels.
    connection = context.connection

    with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        dt_from = datetime.datetime.now().astimezone(
            datetime.timezone.utc
        ) - datetime.timedelta(days=1)
        dt_from_str = dt_from.isoformat()
        cur.execute(
            "SELECT id, access_hash, username, messages_last_queried_at, distance_from_core, collection_priority"
            " FROM telegram.channels_to_query"
            f" WHERE messages_last_queried_at < TIMESTAMP '{dt_from_str}'"
        )
        for record in cur:
            collegram.utils.insert_into_postgres(connection, "channels_to_requery", record)
