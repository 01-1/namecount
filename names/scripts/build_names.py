#!/usr/bin/env python3
"""Build normalized people/fictional-character name CSVs."""

from __future__ import annotations

import argparse
import csv
import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


VALID_TYPES = {"person", "fictional_character"}


@dataclass
class NameRecord:
    name: str
    normalized_name: str
    description: str
    type: str
    source: str
    source_id: str
    language_hint: str
    romanized: bool
    alias_of: str
    confidence: float

    def as_row(self) -> dict:
        return {
            "name": self.name,
            "normalized_name": self.normalized_name,
            "description": self.description,
            "type": self.type,
            "source": self.source,
            "source_id": self.source_id,
            "language_hint": self.language_hint,
            "romanized": str(self.romanized).lower(),
            "alias_of": self.alias_of,
            "confidence": f"{self.confidence:.2f}",
        }


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "")
    value = " ".join(value.strip().split())
    return value


def looks_romanized(value: str) -> bool:
    seen_alpha = False
    for ch in value:
        if ch.isalpha():
            seen_alpha = True
            try:
                if "LATIN" not in unicodedata.name(ch):
                    return False
            except ValueError:
                return False
    return seen_alpha


def is_noise(value: str) -> bool:
    if not value:
        return True
    if len(value) == 1:
        return True
    return False


def split_aliases(raw_aliases: object) -> list[str]:
    if raw_aliases is None:
        return []
    if isinstance(raw_aliases, list):
        return [str(v) for v in raw_aliases if str(v).strip()]
    text = str(raw_aliases).strip()
    if not text:
        return []
    if "|" in text:
        return [part.strip() for part in text.split("|") if part.strip()]
    if "," in text:
        return [part.strip() for part in text.split(",") if part.strip()]
    return [text]


def read_rows(path: Path, fmt: str) -> list[dict]:
    if fmt == "csv":
        with path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))
    if fmt == "jsonl":
        rows = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows
    if fmt == "json":
        return json.loads(path.read_text(encoding="utf-8"))
    raise ValueError(f"Unsupported format: {fmt}")


def type_values(source: dict, row: dict, fields: dict) -> list[str]:
    default_type = source["entity_type"]
    type_field = fields.get("types")
    if not type_field or type_field not in row:
        return [default_type]

    raw_types = row[type_field]
    values = []
    if isinstance(raw_types, list):
        values = [str(v).strip() for v in raw_types]
    else:
        values = [v.strip() for v in str(raw_types).split("|")]
    values = [v for v in values if v in VALID_TYPES]
    return values or [default_type]


def source_records(source: dict, raw_dir: Path) -> Iterable[NameRecord]:
    source_id = source["id"]
    input_cfg = source["input"]
    fmt = input_cfg["format"]
    ext = {"csv": "csv", "jsonl": "jsonl", "json": "json"}[fmt]
    path = raw_dir / f"{source_id}.{ext}"
    fields = source.get("fields", {})
    rows = read_rows(path, fmt)
    for row in rows:
        name_field = fields["name"]
        base_name = normalize_name(str(row.get(name_field, "")))
        if is_noise(base_name):
            continue

        row_source_id = normalize_name(str(row.get(fields.get("source_id", ""), "")))
        row_description = normalize_name(str(row.get(fields.get("description", ""), "")))
        aliases = split_aliases(row.get(fields.get("aliases", ""), None))
        row_types = type_values(source, row, fields)

        for row_type in row_types:
            yield NameRecord(
                name=base_name,
                normalized_name=base_name.casefold(),
                description=row_description,
                type=row_type,
                source=source_id,
                source_id=row_source_id,
                language_hint=source.get("language_hint", "unknown"),
                romanized=looks_romanized(base_name),
                alias_of="",
                confidence=float(source.get("confidence", 0.8)),
            )
            for alias in aliases:
                alias = normalize_name(alias)
                if is_noise(alias):
                    continue
                yield NameRecord(
                    name=alias,
                    normalized_name=alias.casefold(),
                    description=row_description,
                    type=row_type,
                    source=source_id,
                    source_id=row_source_id,
                    language_hint=source.get("language_hint", "unknown"),
                    romanized=looks_romanized(alias),
                    alias_of=base_name,
                    confidence=float(source.get("confidence", 0.8)),
                )


def dedupe(records: Iterable[NameRecord]) -> list[NameRecord]:
    seen = set()
    result = []
    for record in records:
        key = (
            record.type,
            record.normalized_name,
            record.source_id or "",
            record.description,
            record.alias_of,
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(record)
    return result


def write_csv(path: Path, records: list[NameRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        fieldnames = [
            "name",
            "normalized_name",
            "description",
            "type",
            "source",
            "source_id",
            "language_hint",
            "romanized",
            "alias_of",
            "confidence",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record.as_row())


def disambiguated_lines(records: list[NameRecord]) -> list[str]:
    canonical = [r for r in records if not r.alias_of]
    entity_counts: dict[str, set[tuple[str, str, str]]] = {}
    for record in canonical:
        key = record.normalized_name
        entity_key = (record.source, record.source_id or "", record.description or "")
        entity_counts.setdefault(key, set()).add(entity_key)

    lines = []
    for record in sorted(canonical, key=lambda r: (r.normalized_name, r.name, r.description)):
        is_ambiguous = len(entity_counts.get(record.normalized_name, set())) > 1
        if is_ambiguous:
            if record.description:
                line = f"{record.name}, {record.description}"
            elif record.source_id:
                line = f"{record.name}, {record.source_id}"
            else:
                line = record.name
        else:
            line = record.name
        lines.append(line)

    deduped = []
    seen = set()
    for line in lines:
        if line in seen:
            continue
        seen.add(line)
        deduped.append(line)
    return deduped


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(f"{line}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/sources.yaml")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--out-dir", default="data/processed")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)

    all_records = []
    for source in config.get("sources", []):
        if not source.get("enabled", True):
            continue
        all_records.extend(source_records(source, raw_dir))
    all_records = dedupe(all_records)

    people = [r for r in all_records if r.type == "person"]
    fictional = [r for r in all_records if r.type == "fictional_character"]
    disambiguated = disambiguated_lines(all_records)

    write_csv(out_dir / "people_names.csv", people)
    write_csv(out_dir / "fictional_character_names.csv", fictional)
    write_lines(out_dir / "disambiguated_names.txt", disambiguated)
    print(
        "Wrote "
        f"{len(people)} people rows, "
        f"{len(fictional)} fictional rows, and "
        f"{len(disambiguated)} disambiguated lines to {out_dir}"
    )


if __name__ == "__main__":
    main()
