#!/usr/bin/env python3
"""Fetch configured sources into data/raw.

The config file is named .yaml for compatibility with the plan, but is JSON
content so this script can run without external dependencies.
"""

from __future__ import annotations

import argparse
import json
import time
import shutil
import urllib.parse
import urllib.request
import csv
import io
from pathlib import Path


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def file_ext_from_format(fmt: str) -> str:
    mapping = {"csv": "csv", "jsonl": "jsonl", "json": "json"}
    return mapping.get(fmt.lower(), "dat")


def fetch_url(
    url: str,
    timeout_s: int = 60,
    retries: int = 3,
    accept: str = "application/json;q=0.9,*/*;q=0.1",
) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": "namecount/1.0 (data pipeline)",
        },
    )
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as response:
                return response.read()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.5 * (i + 1))
    raise RuntimeError("unreachable")


def fetch_wikidata_sparql(source: dict, out_path: Path) -> Path:
    input_cfg = source["input"]
    endpoint = input_cfg.get("endpoint", "https://query.wikidata.org/sparql")
    query_template = input_cfg["query_template"]
    batch_size = int(input_cfg.get("batch_size", 10000))
    max_rows = int(input_cfg.get("max_rows", 100000))
    sleep_ms = int(input_cfg.get("sleep_ms", 200))

    headers = None
    collected = []
    fetched_rows = 0
    offset = 0

    while fetched_rows < max_rows:
        current_limit = min(batch_size, max_rows - fetched_rows)
        query = (
            query_template.replace("{limit}", str(current_limit))
            .replace("{offset}", str(offset))
        )
        query_url = f"{endpoint}?query={urllib.parse.quote(query)}"
        payload = fetch_url(query_url, accept="text/csv")
        text = payload.decode("utf-8", errors="replace")
        if text.lstrip().startswith("<?xml"):
            raise RuntimeError(
                f"Expected CSV from SPARQL endpoint for {source['id']}, got XML."
            )
        rows = []
        for row in csv.DictReader(io.StringIO(text)):
            if None in row:
                # Skip malformed rows from upstream CSV anomalies.
                continue
            rows.append(row)
        if not rows:
            break
        if headers is None:
            headers = list(rows[0].keys())
        collected.extend(rows)
        count = len(rows)
        fetched_rows += count
        offset += count
        print(f"  fetched {count} rows (total: {fetched_rows}) for {source['id']}")
        if count < current_limit:
            break
        time.sleep(sleep_ms / 1000.0)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if headers is None:
        out_path.write_text("", encoding="utf-8")
        return out_path

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerows(collected)
    return out_path


def fetch_source(source: dict, project_root: Path, raw_dir: Path) -> Path:
    source_id = source["id"]
    input_cfg = source["input"]
    kind = input_cfg["kind"]
    fmt = input_cfg.get("format", "dat")
    out_path = raw_dir / f"{source_id}.{file_ext_from_format(fmt)}"

    if kind == "file":
        src = project_root / input_cfg["path"]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, out_path)
        return out_path

    if kind == "url":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(fetch_url(input_cfg["url"]))
        return out_path

    if kind == "wikidata_sparql":
        if fmt != "csv":
            raise ValueError("wikidata_sparql sources must use csv format")
        fetch_wikidata_sparql(source, out_path)
        return out_path

    raise ValueError(f"Unsupported input.kind: {kind}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="config/sources.yaml",
        help="Path to source config (JSON content with .yaml extension).",
    )
    parser.add_argument("--raw-dir", default="data/raw")
    args = parser.parse_args()

    project_root = Path.cwd()
    config = load_config(project_root / args.config)
    raw_dir = project_root / args.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    fetched = []
    for source in config.get("sources", []):
        if not source.get("enabled", True):
            continue
        fetched_path = fetch_source(source, project_root, raw_dir)
        fetched.append((source["id"], str(fetched_path)))

    print(f"Fetched {len(fetched)} sources:")
    for source_id, path in fetched:
        print(f"- {source_id}: {path}")


if __name__ == "__main__":
    main()
