#!/usr/bin/env python3

"""
# Plex Wrapped!

Step ONE
--------
Extract all content from both databases and combine, cleaning as we go

Step TWO
--------
Start answering questions from gdoc
"""

# stdlib
import sys
import sqlite3

# 3rd party
from pony import orm
import click
import polars as pl

# local
from plexlib import ROOT, DB
from plexlib.data import build_database, fetch_data_from_db
from plexlib.schema import Account, Media, Stream


@click.command()
@click.option("--rebuild", "-r", default=False, type=bool, is_flag=True, help="Rebuild database? Default NO")
def main(rebuild):
    build_database(rebuild)

    # load all media added from last year
    last_year_media_query = "SELECT * FROM media WHERE strftime('%Y', added_date) = '2023'"
    df = load_dataset(last_year_media_query)
    # Durations
    X = (
        df.with_columns(
            [
                pl.when(pl.col("parent_title").is_null())
                .then(pl.col("title"))
                .otherwise(pl.col("parent_title"))
                .alias("name")
            ]
        )
        .group_by(["sourcedb", "media_type"])
        .agg(pl.col("name").count())
        .sort(["sourcedb", "media_type"])
    )
    print(X)


def load_dataset(query):
    df = pl.read_database(
        query=query,
        connection=sqlite3.connect("combined.db"),
        schema_overrides={"rating": float, "audience_rating": float, "year": int},
    )
    return df


if __name__ == "__main__":
    sys.exit(main())
