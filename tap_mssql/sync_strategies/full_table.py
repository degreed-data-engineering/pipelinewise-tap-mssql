#!/usr/bin/env python3
# pylint: disable=duplicate-code,too-many-locals,simplifiable-if-expression

import copy
import csv
import datetime
import json
import pandas as pd
import os
import secrets
import singer
from singer import metadata
from singer import utils
import singer.metrics as metrics
import string
import sys
import tap_mssql.sync_strategies.common as common

from tap_mssql.connection import (
    connect_with_backoff,
    get_azure_sql_engine,
    modify_ouput_converter,
    revert_ouput_converter,
)

LOGGER = singer.get_logger()


def generate_bookmark_keys(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get("replication-method")

    base_bookmark_keys = {
        "last_pk_fetched",
        "max_pk_values",
        "version",
        "initial_full_table_complete",
    }

    bookmark_keys = base_bookmark_keys

    return bookmark_keys



 
def write_dataframe_record(row, catalog_entry, stream_version, columns, table_stream, time_extracted):
    
    rec = row.to_dict() 

    record_message = singer.RecordMessage(
            stream=table_stream,
            record=rec,
            version=stream_version,
            time_extracted=time_extracted,
        )

    singer.write_message(record_message)

def sync_table(mssql_conn, config, catalog_entry, state, columns, stream_version):
    mssql_conn = get_azure_sql_engine(config)
    common.whitelist_bookmark_keys(
        generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
    )

    bookmark = state.get("bookmarks", {}).get(catalog_entry.tap_stream_id, {})
    version_exists = True if "version" in bookmark else False

    initial_full_table_complete = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "initial_full_table_complete"
    )

    state_version = singer.get_bookmark(state, catalog_entry.tap_stream_id, "version")

    table_stream = common.set_schema_mapping(config, catalog_entry.stream)

    activate_version_message = singer.ActivateVersionMessage(
        stream=table_stream, version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (
        version_exists and state_version is None
    ):
        singer.write_message(activate_version_message)

        
        LOGGER.info('**PR** Line 65')
        LOGGER.info(f'bookmark: {bookmark}')
        LOGGER.info(f'version_exists: {version_exists}')
        LOGGER.info(f'initial_full_table_complete: {initial_full_table_complete}')
        LOGGER.info(f'state_version: {state_version}')
        LOGGER.info(f'table_stream: {table_stream}')
        LOGGER.info(f'active_version_message: {activate_version_message}') 
        LOGGER.info(f'catalog_entry: {catalog_entry}')
        LOGGER.info(f'catalog_entry.metadata: {catalog_entry.metadata}')
        LOGGER.info(f'columns: {columns}')
        LOGGER.info(f'stream_version: {stream_version}')
        LOGGER.info(f'state: {state}')


        params = {}

        select_sql = common.generate_select_sql(catalog_entry, columns, fastsync=True)
        #escaped_columns = [common.escape(c) for c in columns]

       # LOGGER.info('**PR 103*** escaped columns')
       # LOGGER.info(escaped_columns)
        columns.extend(['_SDC_EXTRACTED_AT','_SDC_DELETED_AT','_SDC_BATCHED_AT'])
        
        query_df = df = pd.DataFrame(columns=columns) 

        time_extracted = utils.now() 
        conn = mssql_conn.connect().execution_options(stream_results=True)

        csv_saved = 0
        chunk_size = 100000
        files = []
        for chunk_dataframe in pd.read_sql(select_sql, conn, chunksize=chunk_size):
            #LOGGER.info(chunk_dataframe)
            #print(f"Got dataframe w/{len(chunk_dataframe)} rows")
            #query_df = query_df.append(chunk_dataframe, ignore_index=True)
            #LOGGER.info("**PR** line 89 df:")

            csv_saved += 1


            filename = gen_export_filename(table=table_stream)
            filepath = os.path.join('fastsync', filename)
            LOGGER.info('**PR** line 440 filepath')
            LOGGER.info(filepath)

           #query_df.to_csv(f'{filepath}', sep=',', encoding='utf-8',index=False,header=False, compression='gzip')
            chunk_dataframe.to_csv(f'{filepath}', sep=',', encoding='utf-8',index=False,header=False, compression='gzip')
            
            files.append(filename)
            #common.create_gzip(query_df, catalog_entry, csv_saved, table_stream)
            #query_df.apply(write_dataframe_record, args=(catalog_entry,stream_version, columns, table_stream, time_extracted), axis=1)
        

        LOGGER.info('**PR** LINE 141 files')
        LOGGER.info(files)
        singer_message = {'type': 'FASTSYNC','stream':table_stream, 'version': stream_version, 'files':files }
        LOGGER.info('**PR** LINE 125 MESSAGE')
        LOGGER.info(singer_message)
        #singer.write_message(singer_message)
        json_object = json.dumps(singer_message) 
        sys.stdout.write(str(json_object) + '\n')
        sys.stdout.flush()
    else: 

        with mssql_conn.connect() as open_conn:
            LOGGER.info("Generating select_sql")
            select_sql = common.generate_select_sql(catalog_entry, columns)

            params = {}

            if catalog_entry.tap_stream_id == "dbo-InputMetadata":
                prev_converter = modify_ouput_converter(open_conn)

            common.sync_query(
                open_conn,
                catalog_entry,
                state,
                select_sql,
                columns,
                stream_version,
                table_stream,
                params,
            )

            if catalog_entry.tap_stream_id == "dbo-InputMetadata":
                revert_ouput_converter(open_conn, prev_converter)

        # clear max pk value and last pk fetched upon successful sync
        singer.clear_bookmark(state, catalog_entry.tap_stream_id, "max_pk_values")
        singer.clear_bookmark(state, catalog_entry.tap_stream_id, "last_pk_fetched")

        singer.write_message(activate_version_message)



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
    table: str, suffix: str = None, postfix: str = None, ext: str = None
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
#pipelinewise_dbo_UserOrganizations_20220309-213646-973674_batch_tfta0tpw.csv.gz
#pipelinewise_OntologySources_20220321-034721-798507_batch_40L6IJNP.csv.gz
# pipelinewise_{table_stream}_{}
    return 'pipelinewise_{}_{}_batch_{}.{}'.format(
        table, suffix, postfix, ext
    )