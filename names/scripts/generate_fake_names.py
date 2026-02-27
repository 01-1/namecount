#!/usr/bin/env python3
"""Generate fake names with Faker and collision filtering."""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import random
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path


def ensure_vendor_path() -> None:
    project_root = Path(__file__).resolve().parents[1]
    vendor = project_root / ".vendor"
    if vendor.exists():
        sys.path.insert(0, str(vendor))


ensure_vendor_path()
from faker import Faker  # noqa: E402


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "")
    value = " ".join(value.strip().split())
    return value


def normalized_key(value: str) -> str:
    return normalize_name(value).casefold()


def sanitize_candidate(raw: str) -> str:
    text = normalize_name(raw.strip().strip('"').strip("'"))
    text = re.sub(r"^\d+[\.\)\-]\s*", "", text)
    text = re.sub(r"^[\-\*\u2022]\s*", "", text)
    return normalize_name(text)


def is_valid_name(value: str) -> bool:
    if not value or len(value) < 3 or len(value) > 80:
        return False
    if len(value.split()) > 5:
        return False
    return any(ch.isalpha() for ch in value)


def read_blocklist_names(path: Path) -> list[str]:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as fh:
            rows = csv.DictReader(fh)
            names = []
            for row in rows:
                if "name" in row and row["name"].strip():
                    names.append(row["name"])
            return names

    names = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        names.append(line)
        if ", " in line:
            names.append(line.split(", ", 1)[0].strip())
    return names


@dataclass
class NearMatchIndex:
    names: list[str]
    threshold: float
    max_len_diff: int

    def __post_init__(self) -> None:
        self.buckets: dict[str, list[str]] = {}
        for name in self.names:
            key = name[0] if name else ""
            self.buckets.setdefault(key, []).append(name)

    def is_near_match(self, candidate: str) -> bool:
        bucket = self.buckets.get(candidate[0] if candidate else "", [])
        for other in bucket:
            if abs(len(candidate) - len(other)) > self.max_len_diff:
                continue
            if difflib.SequenceMatcher(None, candidate, other).ratio() >= self.threshold:
                return True
        return False

    def add(self, name: str) -> None:
        key = name[0] if name else ""
        self.buckets.setdefault(key, []).append(name)


def build_fakers(locales: list[str], seed: int) -> list[Faker]:
    result = []
    for i, locale in enumerate(locales):
        fake = Faker(locale)
        fake.seed_instance(seed + i)
        result.append(fake)
    return result


def generate_faker_batch(
    fakers: list[Faker], rng: random.Random, style: str, batch_size: int
) -> list[str]:
    names = []
    styles = ["modern", "historical", "fictional", "fantasy"] if style == "mixed" else [style]
    fantasy_suffixes = ["or", "ion", "ath", "iel", "ara", "ius", "wyn"]
    while len(names) < batch_size:
        fake = rng.choice(fakers)
        s = rng.choice(styles)
        if s in {"modern", "historical"}:
            candidate = fake.name()
        elif s == "fictional":
            candidate = f"{fake.first_name()} {fake.last_name()}"
        else:
            first = fake.first_name()
            last = fake.last_name()
            candidate = f"{first}{rng.choice(fantasy_suffixes)} {last}"
        names.append(sanitize_candidate(candidate))
    return names


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/fake_names.yaml")
    parser.add_argument("--output", default="")
    parser.add_argument("--target-count", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1337)
    args = parser.parse_args()

    config = load_config(Path(args.config))
    target_count = args.target_count or int(config.get("target_count", 1000))
    output_path = Path(args.output or config.get("output_path", "data/processed/fake_names.txt"))
    style = config.get("style", "mixed")

    collision_cfg = config.get("collision", {})
    exact_block = bool(collision_cfg.get("exact_block", True))
    near_block = bool(collision_cfg.get("near_match_block", True))
    near_threshold = float(collision_cfg.get("near_match_threshold", 0.92))
    max_len_diff = int(collision_cfg.get("max_len_diff", 3))

    known_names = []
    for source_path in config.get("sources_for_blocklist", []):
        known_names.extend(read_blocklist_names(Path(source_path)))
    known_names = [normalize_name(x) for x in known_names if normalize_name(x)]
    known_norm = [normalized_key(x) for x in known_names]
    exact_set = set(known_norm) if exact_block else set()

    near_index = NearMatchIndex(known_norm, near_threshold, max_len_diff)
    accepted: list[str] = []
    accepted_norm: set[str] = set()

    faker_cfg = config.get("faker", {})
    batch_size = int(faker_cfg.get("batch_size", 250))
    locales = faker_cfg.get(
        "locales",
        [
            "en_US",
            "en_GB",
            "fr_FR",
            "es_ES",
            "de_DE",
            "it_IT",
            "pt_BR",
            "nl_NL",
            "sv_SE",
            "pl_PL",
            "cs_CZ",
            "ro_RO",
        ],
    )

    max_attempts = int(config.get("max_attempts", 4000))
    rng = random.Random(args.seed)
    fakers = build_fakers(locales, seed=args.seed)

    attempts = 0
    while len(accepted) < target_count and attempts < max_attempts:
        attempts += 1
        candidates = generate_faker_batch(
            fakers=fakers,
            rng=rng,
            style=style,
            batch_size=batch_size,
        )

        for candidate in candidates:
            if not is_valid_name(candidate):
                continue
            key = normalized_key(candidate)
            if key in accepted_norm:
                continue
            if exact_block and key in exact_set:
                continue
            if near_block and near_index.is_near_match(key):
                continue

            accepted.append(candidate)
            accepted_norm.add(key)
            if near_block:
                near_index.add(key)
            if len(accepted) >= target_count:
                break

    if len(accepted) < target_count:
        raise RuntimeError(
            f"Could not reach target_count={target_count}. Accepted={len(accepted)}."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for name in accepted:
            fh.write(f"{name}\n")
    print(f"Wrote {len(accepted)} fake names to {output_path}")


if __name__ == "__main__":
    main()
