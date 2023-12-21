import datetime as dt

from pony import orm

db = orm.Database()


class Account(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    sourcedb = orm.Required(str)
    originalid = orm.Required(int)
    name = orm.Required(str)

    streams = orm.Set("Stream")
    super_account = orm.Optional("SuperAccount")

    orm.composite_index(sourcedb, originalid)


class SuperAccount(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str)

    plex_accounts = orm.Set("Account")
    streams = orm.Set("Stream")


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
    super_media = orm.Optional("SuperMedia")

    orm.composite_index(sourcedb, originalid)
    orm.composite_index(media_type, sourcedb)

    @property
    def formatted_name(self):
        if self.parent_title is None:
            return self.title
        return f"{self.title} ({self.parent_title})"


class SuperMedia(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
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
    plex_media = orm.Set("Media")

    orm.composite_index(title, parent_title)

    @property
    def formatted_name(self):
        if self.parent_title is None:
            return self.title
        return f"{self.title} ({self.parent_title})"


class Stream(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    sourcedb = orm.Required(str)
    original_media_id = orm.Required(int)
    ts = orm.Required(dt.datetime)
    original_account_id = orm.Required(int)
    device_name = orm.Optional(str)
    device_platform = orm.Optional(str)

    account = orm.Optional("Account")
    media = orm.Optional("Media")
    super_account = orm.Optional("SuperAccount")
    super_media = orm.Optional("SuperMedia")
