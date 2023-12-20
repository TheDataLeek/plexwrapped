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

    streams = orm.Set("Stream")

    orm.composite_index(sourcedb, originalid)


class Stream(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    sourcedb = orm.Required(str)
    ts = orm.Required(dt.datetime)
    original_media_id = orm.Required(int)
    duration = orm.Required(int)

    account = orm.Optional("Account")
    media = orm.Optional("Media")
