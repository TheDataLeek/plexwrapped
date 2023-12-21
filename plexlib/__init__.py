import os
import pathlib

ROOT = pathlib.Path()
DB = ROOT / "combined.db"
OUTPUT = ROOT / "output"

if not OUTPUT.exists():
    os.mkdir(OUTPUT)
