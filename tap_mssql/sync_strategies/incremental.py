#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from singer import metadata

from tap_mssql.connection import (
    connect_with_backoff,
    get_azure_sql_engine,
)
import tap_mssql.sync_strategies.common as common

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {"replication_key", "replication_key_value", "version"}


def sync_table(mssql_conn, config, catalog_entry, state, columns):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get("replication-key")
    replication_key_state = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "replication_key"
    )

    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(
            state, catalog_entry.tap_stream_id, "replication_key_value"
        )
    else:
        state = singer.write_bookmark(
            state,
            catalog_entry.tap_stream_id,
            "replication_key",
            replication_key_metadata,
        )
        state = singer.clear_bookmark(
            state, catalog_entry.tap_stream_id, "replication_key_value"
        )

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(
        state, catalog_entry.tap_stream_id, "version", stream_version
    )

    table_stream = common.set_schema_mapping(config, catalog_entry.stream)

    activate_version_message = singer.ActivateVersionMessage(
        stream=table_stream, version=stream_version
    )

    singer.write_message(activate_version_message)
    LOGGER.info("Beginning SQL")
    with mssql_conn.connect() as open_conn:
        dry_run_limit = config.get("dry_run_limit")
        select_sql = common.generate_select_sql(catalog_entry, columns, dry_run_limit)
        params = {}

        start_replication_key_value = config.get("start_replication_key_value")
        end_replication_key_value = config.get("end_replication_key_value")

        is_datetime_key = (
            catalog_entry.schema.properties[replication_key_metadata].format == "date-time"
        ) if replication_key_metadata else False

        # Use config start value when state has no bookmark (first run of a range partition).
        # Config values arrive as strings from env vars — cast non-datetime keys to int.
        if replication_key_value is None and start_replication_key_value is not None:
            replication_key_value = (
                start_replication_key_value
                if is_datetime_key
                else int(start_replication_key_value)
            )

        if replication_key_value is not None:
            if is_datetime_key:
                replication_key_value = pendulum.parse(replication_key_value)

            select_sql += ' WHERE "{}" >= ?'.format(replication_key_metadata)
            params["replication_key_value"] = replication_key_value

            if end_replication_key_value is not None:
                select_sql += ' AND "{}" <= ?'.format(replication_key_metadata)
                params["end_replication_key_value"] = (
                    pendulum.parse(end_replication_key_value)
                    if is_datetime_key
                    else int(end_replication_key_value)
                )

            select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)
        elif replication_key_metadata is not None:
            if end_replication_key_value is not None:
                select_sql += ' WHERE "{}" <= ?'.format(replication_key_metadata)
                params["end_replication_key_value"] = (
                    pendulum.parse(end_replication_key_value)
                    if is_datetime_key
                    else int(end_replication_key_value)
                )

            select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)

        common.sync_query(
            open_conn, catalog_entry, state, select_sql, columns, stream_version, table_stream, params
        )
