"""Validate every PBIR JSON file against the official Microsoft JSON schemas.

    .venv\\Scripts\\python powerbi\\tools\\validate_pbir.py

Schemas (and the files they $ref) are downloaded once and cached in powerbi/tools/.schema_cache.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import requests
from jsonschema import Draft7Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT7

ROOT = Path(__file__).resolve().parents[1]
CACHE = Path(__file__).resolve().parent / ".schema_cache"
CACHE.mkdir(exist_ok=True)


def fetch(uri: str):
    f = CACHE / (hashlib.md5(uri.encode()).hexdigest() + ".json")
    if not f.exists():
        r = requests.get(uri, timeout=60)
        r.raise_for_status()
        f.write_bytes(r.content)
    return json.loads(f.read_text(encoding="utf-8-sig"))


def retrieve(uri: str):
    return Resource.from_contents(fetch(uri), default_specification=DRAFT7)


def main() -> int:
    registry = Registry(retrieve=retrieve)
    errors = 0
    files = [p for p in ROOT.rglob("*.json") if ".Report" in str(p) and "StaticResources" not in str(p)]
    files += list(ROOT.glob("*.pbip"))
    for f in sorted(files):
        doc = json.loads(f.read_text(encoding="utf-8"))
        uri = doc.get("$schema")
        if not uri:
            continue
        schema = fetch(uri)
        v = Draft7Validator(schema, registry=registry)
        errs = sorted(v.iter_errors(doc), key=lambda e: list(e.path))
        for e in errs[:5]:
            errors += 1
            print(f"{f.relative_to(ROOT)} :: {'/'.join(map(str, e.path))} :: {e.message[:220]}")
    print(f"checked {len(files)} files, {errors} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
