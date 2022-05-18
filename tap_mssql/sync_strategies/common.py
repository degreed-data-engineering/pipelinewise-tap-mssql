#!/usr/bin/env python3
# pylint: disable=too-many-arguments,duplicate-code,too-many-locals

from ast import Num
import copy
import csv
import datetime
import glob
import multiprocessing 
import os
import string
import secrets
import singer
import time
import uuid

from . import split_gzip

import singer.metrics as metrics
from singer import metadata
from singer import utils

LOGGER = singer.get_logger()
 

def escape(string):
    if "`" in string:
        raise Exception(
            "Can't escape identifier {} because it contains a double quote".format(
                string
            )
        )
    return '"' + string + '"'


def set_schema_mapping(config, stream):
    schema_mapping = config.get("include_schemas_in_destination_stream_name")

    if schema_mapping:
        stream = stream.replace("-", "_")
    return stream


def generate_tap_stream_id(table_schema, table_name):
    return table_schema + "-" + table_name


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, "version")

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), "selected")

    return selected_md


def property_is_selected(stream, property_name):
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ("properties", property_name), "inclusion"),
        metadata.get(md_map, ("properties", property_name), "selected"),
        True,
    )


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get("is-view")


def get_database_name(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get("database-name")


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get("view-key-properties", [])
    else:
        key_properties = stream_metadata.get("table-key-properties", [])

    return key_properties


def generate_select_sql(catalog_entry, columns):
    database_name = get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = [escape(c) for c in columns]

    select_sql = "SELECT {} FROM {}.{}".format(
        ",".join(escaped_columns), escaped_db, escaped_table
    )

    # extracted_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    # batched_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    # select_sql = "SELECT {}, '{}' AS _SDC_EXTRACTED_AT, '{}' AS _SDC_BATCHED_AT, null AS _SDC_DELETED_AT FROM {}.{}".format(
    #     ",".join(escaped_columns), extracted_at, batched_at, escaped_db, escaped_table
    # )

    # escape percent signs
    select_sql = select_sql.replace("%", "%%")
    return select_sql


def row_to_singer_record(
    catalog_entry, version, table_stream, row, columns, time_extracted
):
    row_to_persist = ()
    md_map = metadata.to_map(catalog_entry.metadata)
    md_map[("properties", "_sdc_deleted_at")] = {
        "sql-datatype": "datetime"  # maybe datetimeoffset??
    }
    for idx, elem in enumerate(row):
        # property_type = catalog_entry.schema.properties[columns[idx]].type
        property_type = md_map.get(("properties", columns[idx])).get("sql-datatype")
        if isinstance(elem, datetime.datetime):
            row_to_persist += (elem.isoformat() + "+00:00",)

        elif isinstance(elem, datetime.date):
            row_to_persist += (elem.isoformat() + "T00:00:00+00:00",)

        elif isinstance(elem, datetime.timedelta):
            epoch = datetime.datetime.utcfromtimestamp(0)
            timedelta_from_epoch = epoch + elem
            row_to_persist += (timedelta_from_epoch.isoformat() + "+00:00",)

        elif isinstance(elem, bytes):
            if property_type in ["binary", "varbinary"]:
                # Convert binary byte array to hex string‘
                hex_representation = f"0x{elem.hex().upper()}"
                row_to_persist += (hex_representation,)
            else:
                # for BIT value, treat 0 as False and anything else as True
                boolean_representation = elem != b"\x00"
                row_to_persist += (boolean_representation,)

        elif "boolean" in property_type or property_type == "boolean":
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)
        elif isinstance(elem, uuid.UUID):
            row_to_persist += (str(elem),)
        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=table_stream,
        record=rec,
        version=version,
        time_extracted=time_extracted,
    )


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bk in [
        non_whitelisted_bookmark_key
        for non_whitelisted_bookmark_key in state.get("bookmarks", {})
        .get(tap_stream_id, {})
        .keys()
        if non_whitelisted_bookmark_key not in bookmark_key_set
    ]:
        singer.clear_bookmark(state, tap_stream_id, bk)



def generate_random_string(length: int = 8) -> str:
    """
    Generate cryptographically secure random strings
    Uses best practice from Python doc https://docs.python.org/3/library/secrets.html#recipes-and-best-practices
    Args:
        length: length of the string to generate
    Returns: random string
    """

    if length < 1:
        raise Exception('Length must be at least 1!')

    if 0 < length < 8:
        warnings.warn('Length is too small! consider 8 or more characters')

    return ''.join(
        secrets.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def gen_export_filename(
    tap_id: str, table: str, suffix: str = None, postfix: str = None, ext: str = None
) -> str:
    """
    Generates a unique filename used for exported fastsync data that avoids file name collision

    Default pattern:
        pipelinewise_<tap_id>_<table>_<timestamp_with_ms>_fastsync_<random_string>.csv.gz

    Args:
        tap_id: Unique tap id
        table: Name of the table to export
        suffix: Generated filename suffix. Defaults to current timestamp in milliseconds
        postfix: Generated filename postfix. Defaults to a random 8 character length string
        ext: Filename extension. Defaults to .csv.gz

    Returns:
        Unique filename as a string
    """
    if not suffix:
        suffix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')

    if not postfix:
        postfix = generate_random_string()

    if not ext:
        ext = 'csv.gz'

    return 'pipelinewise_{}_{}_{}_fastsync_{}.{}'.format(
        tap_id, table, suffix, postfix, ext
    )

def sync_query(
    cursor,
    catalog_entry,
    state,
    select_sql,
    columns,
    stream_version,
    table_stream,
    params,
    config,
):
    replication_key = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "replication_key"
    )




    # query_string = cursor.mogrify(select_sql, params)

    time_extracted = utils.now()
    if len(params) == 0:
        results = cursor.execute(select_sql)

        # LOGGER.info("**PR** LINE 190 results")
        # LOGGER.info(results)
    else:
        results = cursor.execute(select_sql, params["replication_key_value"])
        # LOGGER.info("**PR** LINE 194 results")
        # LOGGER.info(results)
 
    


    
    #cur.execute(sql) has happened at this point
 

    filename = gen_export_filename(tap_id='test', table=catalog_entry.table)
    filepath = os.path.join('tmp', filename)
    

    # export_batch_rows = self.connection_config['export_batch_rows'] TODO: put this back so its using the config value stated.

    export_batch_rows = config.get("export_batch_rows")

    exported_rows = 0
    split_large_files=True
    split_file_chunk_size_mb=1000
    split_file_max_chunks=20
 
    gzip_splitter = split_gzip.open(
        filepath, 
        mode='wt',
        chunk_size_mb=split_file_chunk_size_mb,
        max_chunks=split_file_max_chunks if split_large_files else 0 
        #compress=compress,
        )

    with gzip_splitter as split_gzip_files:
        writer = csv.writer(
            split_gzip_files,
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        while True:
            rows = results.fetchmany(export_batch_rows) # TODO: change to config 
            # No more rows to fetch, stop loop
            if not rows:
                break

            # Log export status
            exported_rows += len(rows)
            if len(rows) == export_batch_rows:
                # Then we believe this to be just an interim batch and not the final one so report on progress

                LOGGER.info(
                    'Exporting batch from %s to %s rows from %s...',
                    (exported_rows - export_batch_rows),
                    exported_rows,
                    catalog_entry.table 
                )
            # Write rows to file in one go
            writer.writerows(rows)

        LOGGER.info(
            'Exported total of %s rows from %s...', exported_rows, catalog_entry.table
        )

 


    rows = results.fetchall()
    number_of_rows = len(rows)
    rows_saved = 0
     
    
    # LOGGER.info("**PR** LINE 204 row")
    # LOGGER.info(rows)
    #row = results.fetchall()

    
    # LOGGER.info("**PR** LINE 200 row")
    # LOGGER.info(row)
    # LOGGER.info("**PR** LINE 211 number_of_rows ")
    # LOGGER.info(number_of_rows)


    database_name = get_database_name(catalog_entry)

    md_map = metadata.to_map(catalog_entry.metadata) 

    # update replication method

    stream_metadata = md_map.get((), {}) 

    stream_metadata.update({'replication-method':'FASTSYNC'})
    replication_method = stream_metadata.get("replication-method")    

    # LOGGER.info("**PR** LINE 231 md_map ")
    # LOGGER.info(md_map)
    # LOGGER.info("**PR** LINE 233 stream_metadata ")
    # LOGGER.info(stream_metadata)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
