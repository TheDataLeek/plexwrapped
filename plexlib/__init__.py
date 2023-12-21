# stdlib
import os
import pathlib
import functools

# 3rd party
import polars as pl

ROOT = pathlib.Path()
DB = ROOT / "combined.db"
OUTPUT = ROOT / "output"

if not OUTPUT.exists():
    os.mkdir(OUTPUT)


def with_cache(func):
    @functools.wraps(func)
    def _(*args, **kwargs):
        cachefile = OUTPUT / f"cache_{func.__name__}.feather"
        if not cachefile.exists():
            df: pl.DataFrame = func(*args, **kwargs)
            df.columns = [c.lower() for c in df.columns]
            df.write_ipc(cachefile)
        else:
            df = pl.read_ipc(cachefile)

        return df

    return _
