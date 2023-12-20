import datetime as dt

from pony import orm

db = orm.Database()


class Account(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    sourcedb = orm.Required(str)
    originalid = orm.Required(int)
    name = orm.Required(str)

    streams = orm.Set("Stream")

    orm.composite_index(sourcedb, originalid)


class Media(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    sourcedb = orm.Required(str)
    originalid = orm.Required(int)
    title = orm.Required(str)
    parent_title = orm.Optional(str)
    media_type = orm.Optional(str)
    studio = orm.Optional(str)
    rating = orm.Optional(float)
    audience_rating = orm.Optional(float)
    content_rating = orm.Optional(str)
    duration_minutes = orm.Optional(int)
    summary = orm.Optional(str)
    year = orm.Optional(int)
    release_date = orm.Optional(dt.date)
    added_date = orm.Optional(dt.date)
    tags_genre = orm.Optional(str)
    tags_director = orm.Optional(str)
    tags_writer = orm.Optional(str)
    tags_star = orm.Optional(str)
    tags_country = orm.Optional(str)

    streams = orm.Set("Stream")

    orm.composite_index(sourcedb, originalid)

    @property
    def genre(self) -> list:
        print(self.tags_genre)
        return self.tags_genre.split("|")

    @property
    def director(self) -> list:
        return self.tags_director.split("|")

    @property
    def writer(self) -> list:
        return self.tags_writer.split("|")

    @property
    def star(self) -> list:
        return self.tags_star.split("|")

    @property
    def country(self) -> list:
        return self.tags_country.split("|")


class Stream(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    sourcedb = orm.Required(str)
    ts = orm.Required(dt.datetime)
    original_media_id = orm.Required(int)
    duration = orm.Required(int)

    account = orm.Optional("Account")
    media = orm.Optional("Media")
