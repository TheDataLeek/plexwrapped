# stdlib
import os
import pathlib
import sqlite3

# 3rd party
from pony import orm

# local
from plexlib.schema import db, Media, Account, Stream
from . import ROOT, DB


def build_database(rebuild):
    if not rebuild:
        db.bind(provider="sqlite", filename=str(DB.absolute()), create_db=True)
        db.generate_mapping(create_tables=True)
        return

    if DB.exists():
        os.remove(DB)

    db.bind(provider="sqlite", filename=str(DB.absolute()), create_db=True)
    db.generate_mapping(create_tables=True)

    for source_db in pathlib.Path().glob("*.db"):
        # first, snag and align accounts
        base_name = source_db.stem
        if base_name == "combined":
            continue

        print(f"Extracting from {base_name}")
        extract_accounts(source_db)
        extract_media(source_db)
        extract_streams(source_db)


@orm.db_session
def extract_accounts(source_db):
    query = """
        SELECT id AS originalid, name
        FROM main.accounts
        WHERE name != ''
    """
    for row in fetch_data_from_db(source_db, query):
        Account(**row)

    db.commit()


@orm.db_session
def extract_media(source_db):
    query = """
      SELECT
        mi.id AS originalid
      , mti1.title
      , mti3.title AS parent_title
      , CASE
          WHEN mti1.metadata_type = 1 THEN 'film'
          WHEN mti1.metadata_type = 4 THEN 'episode'
          WHEN mti1.metadata_type IN (2, 3) THEN 'series'
        END AS media_type
      , mti1.studio
      , mti1.rating
      , mti1.audience_rating
      , mti1.content_rating
      , ((mi.duration / 1000) / 60) duration_minutes
      , mti1.summary
      , mti1.year
      , DATE(mti1.originally_available_at, 'unixepoch', 'localtime') AS release_date
      , DATE(mti1.added_at, 'unixepoch', 'localtime') AS added_date
      , mti1.tags_genre
      , mti1.tags_director
      , mti1.tags_writer
      , mti1.tags_star
      , mti1.tags_country
      FROM media_items mi
      LEFT JOIN metadata_items mti1
        ON mi.metadata_item_id = mti1.id
      LEFT JOIN metadata_items mti2
        ON mti1.parent_id = mti2.id
      LEFT JOIN metadata_items mti3
        ON mti2.parent_id = mti3.id
      WHERE mti1.title IS NOT NULL
        AND mti1.title != ''
        AND media_type IN ('film', 'episode')
        AND mti1.deleted_at IS NULL
    """

    for row in fetch_data_from_db(source_db, query):
        Media(**row)

    db.commit()


@orm.db_session
def extract_streams(source_db):
    query = """
        SELECT
          DATETIME(media_streams.created_at, 'unixepoch', 'localtime') AS ts
        , metadata_items.id AS original_media_id
        , media_parts.duration
        FROM media_streams
        LEFT JOIN stream_types st
          ON media_streams.stream_type_id = st.id
        LEFT JOIN media_parts
          ON media_streams.media_part_id = media_parts.id
        LEFT JOIN media_items
          ON media_items.id = media_streams.media_item_id
        LEFT JOIN metadata_items
          ON metadata_items.id = media_items.metadata_item_id
        WHERE ts BETWEEN DATETIME('2023-01-01 00:00:00') AND CURRENT_TIMESTAMP
          AND st.name = 'video'
    """

    for row in fetch_data_from_db(source_db, query):
        s = Stream(**row)

        media_id = row["original_media_id"]
        sourcedb = row["sourcedb"]
        if media_id is not None and sourcedb is not None:
            media = Media.get(sourcedb=sourcedb, originalid=media_id)
            s.media = media

    db.commit()


def fetch_data_from_db(dbfile, query):
    base_name = dbfile.stem
    cdb = sqlite3.connect(dbfile)
    ccur = cdb.cursor()
    ccur.execute(query)

    cols = [c[0] for c in ccur.description]
    for row in ccur.fetchall():
        yield {"sourcedb": base_name, **{k: v for k, v in zip(cols, row) if v is not None and v != ""}}

    ccur.close()
    cdb.close()
