# stdlib
import os
import pathlib
import sqlite3

# 3rd party
from pony import orm
from thefuzz import fuzz
from tqdm import tqdm

# local
from plexlib.schema import db, Media, Account, Stream, SuperAccount, SuperMedia
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

    # Need to combine overlapping identities over databases
    combine_accounts()
    # Need to combine overlapping media over databases
    combine_media()


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
          media_items.id AS original_media_id
        , DATETIME(viewed_at, 'unixepoch', 'localtime') AS ts
        , a.id AS original_account_id
        , devices.name AS device_name
        , devices.platform AS device_platform
        FROM metadata_item_views miv
        LEFT JOIN accounts a
          ON a.id = miv.account_id
        LEFT JOIN library_sections ls
          ON ls.id = miv.library_section_id
        LEFT JOIN metadata_items mi
          ON mi.guid = miv.guid
        LEFT JOIN media_items
          ON media_items.metadata_item_id = mi.id
        LEFT JOIN devices
          ON miv.device_id = devices.id
        WHERE DATETIME(viewed_at, 'unixepoch', 'localtime') BETWEEN '2023-01-01' AND '2023-12-31'
          AND original_media_id IS NOT NULL
    """

    for row in fetch_data_from_db(source_db, query):
        s = Stream(**row)

        media_id = row["original_media_id"]
        account_id = row["original_account_id"]
        sourcedb = row["sourcedb"]

        if media_id is not None and sourcedb is not None:
            media = Media.get(sourcedb=sourcedb, originalid=media_id)
            s.media = media

        if account_id is not None and sourcedb is not None:
            account = Account.get(sourcedb=sourcedb, originalid=account_id)
            s.account = account

    db.commit()


@orm.db_session
def combine_accounts():
    for account1 in Account.select():
        for account2 in Account.select():
            if account1 == account2:
                continue
            base_name = account1.name
            if fuzz.ratio(base_name, account2.name) >= 95 and SuperAccount.get(name=base_name) is None:
                s = SuperAccount(name=account1.name)
                account1.super_account = s
                account2.super_account = s
                for stream in account1.streams:
                    stream.super_account = s
                for stream in account2.streams:
                    stream.super_account = s

                print(
                    f"Created SuperAccount[{s.name}] with "
                    f"{account1.name}@{account1.sourcedb} and {account2.name}@{account2.sourcedb}"
                )

    for account in Account.select():
        if account.super_account is None:
            SuperAccount(name=account.name)

    db.commit()


@orm.db_session
def combine_media():
    cols_to_ignore = ("sourcedb", "originalid", "streams", "super_media")
    fuzz_threshold = 85
    all_media = Media.select()[:]
    for media1 in tqdm(all_media, total=len(all_media), desc="Combining Media"):
        for media2 in all_media:
            # don't create based on the same one
            if media1 == media2:
                continue

            # if one's a show, the other is movie? skip it
            if media1.media_type != media2.media_type:
                continue

            base_title = media1.title
            base_parent_title = media1.parent_title

            same_title = fuzz.ratio(base_title, media2.title) >= fuzz_threshold
            if not same_title:
                continue

            same_show = (base_parent_title is None and media2.parent_title is None) or (
                fuzz.ratio(base_parent_title, media2.parent_title) >= fuzz_threshold
            )
            if not same_show:
                continue

            super_media_exists = SuperMedia.get(title=base_title, parent_title=base_parent_title) is not None
            if super_media_exists:
                continue

            s = SuperMedia(**{k: v for k, v in media1.to_dict().items() if k not in cols_to_ignore})
            media1.super_media = s
            media2.super_media = s
            for stream in media1.streams:
                stream.super_media = s
            for stream in media2.streams:
                stream.super_media = s

            print(
                f"Created SuperMedia[{s.formatted_name}] with "
                f"{media1.formatted_name}@{media1.sourcedb} and {media2.formatted_name}@{media2.sourcedb}"
            )

    for media in Media.select():
        if media.super_media is None:
            SuperMedia(**{k: v for k, v in media.to_dict().items() if k not in cols_to_ignore})

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
