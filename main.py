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
import os
import sqlite3
import functools
import datetime as dt

# 3rd party
from pony import orm
import click
import polars as pl
import numpy as np
import snowflake
import snowflake.connector

# local
from plexlib import ROOT, DB, OUTPUT, with_cache
from plexlib.data import build_database, fetch_data_from_db
from plexlib.schema import Account, Media, Stream


@click.command()
def main():
    """
    PLEX WRAPPED!!

    Ended up importing all data to Snowflake so this will just primarily focus on
    ingestion, caching, and then answering all the questions!
    """
    # build_database(rebuild)

    df = load_dataset(
        """
        SELECT *
        FROM plex_wrapped.output.plex_wrapped_2023_base
    """
    )
    print(sorted(df.columns))

    print(sorted(df.get_column("user").unique()))

    # print(df.filter((pl.col("user") == "zo347") & (pl.col("media_type") != "TV Series")))
    # return
    # owner_stats()
    individual_stats(df)
    group_stats(df)


def group_stats(df: pl.DataFrame):
    """
    Most popular show
    Most popular movie
    Most popular genre
    Top Actor
    Who watched the most past 1am
    Who watched the most during work hours on weekdays (6a - 5p)
    Percent of users who watch with subtitles
    """
    print("Top show")
    X = (
        df.filter((pl.col("viewed_at") >= dt.date(2023, 1, 1)) & (pl.col("media_type") == "TV Series"))
        .group_by("grandparent_title")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort("duration_hours")
    )
    print(X)

    print("Top movie")
    X = (
        df.filter((pl.col("viewed_at") >= dt.date(2023, 1, 1)) & (pl.col("media_type") != "TV Series"))
        .group_by("title")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort("duration_hours")
    )
    print(X)

    print("Top genre")
    X = stats_by_tag(df, "tags_genre")
    print(X)

    print("Top star")
    X = stats_by_tag(df, "tags_star")
    print(X)

    print("Top director")
    X = stats_by_tag(df, "tags_director")
    print(X)

    print("Top country")
    X = stats_by_tag(df, "tags_country")
    print(X)

    print("Watched most past 1am MST")
    X = (
        df.filter(pl.col("viewed_at") >= dt.date(2023, 1, 1))
        .with_columns(sleepy_time=pl.col("viewed_at").dt.hour().is_between(1, 6, closed="both"))
        .filter("sleepy_time")
        .group_by("user")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort("duration_hours")
    )
    print(X)

    print("Watched most during work hours on weekdays")
    X = (
        df.filter(pl.col("viewed_at") >= dt.date(2023, 1, 1))
        .with_columns(
            during_work_hours=(
                pl.col("viewed_at").dt.hour().is_between(8, 17, closed="both")
                & pl.col("viewed_at").dt.weekday().is_between(1, 5, closed="both")
            )
        )
        .filter("during_work_hours")
        .group_by("user")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort("duration_hours")
    )
    print(X)


def individual_stats(df: pl.DataFrame, user=None):
    if user is not None:
        df = df.filter(pl.col("user") == user)

    print("Total watch time")
    X = (
        df.filter(pl.col("viewed_at") >= dt.date(2023, 1, 1))
        .group_by("user")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .with_columns(duration_days=pl.col("duration_hours") / 24)
        .sort("duration_hours")
    )
    print(X)

    print("Top show by user")
    X = (
        df.filter((pl.col("viewed_at") >= dt.date(2023, 1, 1)) & (pl.col("media_type") == "TV Series"))
        .group_by("user", "grandparent_title")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort("user", "duration_hours")
    )
    print(X)

    print("Top movie by user")
    X = (
        df.filter((pl.col("viewed_at") >= dt.date(2023, 1, 1)) & (pl.col("media_type") != "TV Series"))
        .group_by("user", "title")
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort("user", "duration_hours")
    )
    print(X)

    print("Top genre by user")
    X = stats_by_tag(df, "tags_genre", by=["user"])
    print(X)

    print("Top star by user")
    X = stats_by_tag(df, "tags_star", by=["user"])
    print(X)

    print("Top director by user")
    X = stats_by_tag(df, "tags_director", by=["user"])
    print(X)

    print("Top country by user")
    X = stats_by_tag(df, "tags_country", by=["user"])
    print(X)


def stats_by_tag(df: pl.DataFrame, column: str, by: list[str] = None) -> pl.DataFrame:
    if by is None:
        by = []
    newcol = column.split("_")[-1]
    X = (
        df.filter(
            (pl.col("viewed_at") >= dt.date(2023, 1, 1))
            & (pl.col(column).is_not_null())
            & (pl.col("media_type") != "TV Series")
        )
        .with_columns(pl.col(column).str.split("|").alias(newcol))
        .explode(newcol)
        .group_by(*by, newcol)
        .agg((pl.col("duration_minutes").sum() / 60).alias("duration_hours"))
        .sort(*by, "duration_hours")
    )
    return X


def owner_stats():
    # load all media added from last year
    last_year_media_query = "SELECT * FROM media WHERE strftime('%Y', added_date) = '2023'"
    df = load_dataset(last_year_media_query)

    # Media Added
    X = (
        df.with_columns(
            [
                (
                    pl.when(pl.col("parent_title").is_null() | (pl.col("parent_title") == ""))
                    .then(pl.col("title"))
                    .otherwise(pl.col("parent_title"))
                    .alias("name")
                ),
            ]
        )
        .group_by(["sourcedb", "media_type"])
        .agg(
            pl.col("name").count().alias("# Added"),
            pl.col("duration_minutes").sum(),
            pl.col("rating").drop_nans().drop_nulls().mean(),
            pl.col("audience_rating").drop_nans().drop_nulls().mean(),
        )
        .with_columns((pl.col("duration_minutes") / 60).alias("hours"))
        .sort("sourcedb", "media_type")
    )
    print(X)

    # media added by genre
    X = (
        df.with_columns(
            [
                (
                    pl.when(pl.col("parent_title").is_null() | (pl.col("parent_title") == ""))
                    .then(pl.col("title"))
                    .otherwise(pl.col("parent_title"))
                    .alias("name")
                ),
                (pl.col("tags_genre").replace("", "UNK").str.split("|").alias("genres")),
            ]
        )
        .with_columns((pl.col("genres").list.first().alias("genre")))
        .group_by(["sourcedb", "media_type", "genre"])
        .agg(
            pl.col("name").count().alias("# Added"),
            pl.col("duration_minutes").sum(),
            pl.col("rating").drop_nans().drop_nulls().mean(),
            pl.col("audience_rating").drop_nans().drop_nulls().mean(),
        )
        .with_columns((pl.col("duration_minutes") / 60).alias("hours"))
        .sort("sourcedb", "media_type", "audience_rating")
    )
    print(X)
    X.write_csv(OUTPUT / "added_by_genre.csv")


@with_cache
def load_dataset(query):
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    passwd = os.environ["SNOWFLAKE_PASSWORD"]

    conn = snowflake.connector.connect(account=account, user=user, password=passwd)

    df = pl.read_database(
        query=query,
        connection=conn,
    )

    return df


if __name__ == "__main__":
    sys.exit(main())
