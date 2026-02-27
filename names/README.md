# namecount

Pipeline to build normalized name lists for:
- real people (`people_names.csv`)
- fictional characters (`fictional_character_names.csv`)

Each output row includes a `description` column to help disambiguate identical names.

## Quick start

```bash
python scripts/fetch_sources.py
python scripts/build_names.py
```

Outputs:
- `data/processed/people_names.csv`
- `data/processed/fictional_character_names.csv`
- `data/processed/disambiguated_names.txt`
- `data/processed/fake_names.txt`

`disambiguated_names.txt` is newline-delimited:
- unambiguous names are plain (for example `Shinzo Abe`)
- ambiguous names are emitted as `Name, description` (for example `Eumedes, son of Melas`)

## Config

Production sources are defined in `config/sources.yaml` (JSON content for no-dependency parsing).
Test fixtures are in `config/sources.test.yaml`.

Each source defines:
- `entity_type`: `person` or `fictional_character`
- `input.kind`: `file`, `url`, or `wikidata_sparql`
- `input.format`: `csv`, `jsonl`, or `json`
- `fields`: source-specific field mapping (`name`, `source_id`, optional `description`, optional `aliases`, optional `types`)

`wikidata_sparql` sources support paginated queries with:
- `query_template` containing `{limit}` and `{offset}`
- `batch_size`, `max_rows`, and `sleep_ms`

## Tests

```bash
pytest -q
```

If you want fixture-only execution without network:
```bash
python scripts/fetch_sources.py --config config/sources.test.yaml
python scripts/build_names.py --config config/sources.test.yaml
```

## Fake names

The fake-name generator uses the `Faker` library with multi-locale sampling and
exact + near-match filtering against your existing name outputs.

```bash
python scripts/generate_fake_names.py --config config/fake_names.yaml
```

Test config:
```bash
python scripts/generate_fake_names.py --config config/fake_names.test.yaml
```
