from pathlib import Path
import csv
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
TEST_CONFIG = ROOT / "config/sources.test.yaml"


def run_pipeline(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw_dir = tmp_path / "raw"
    out_dir = tmp_path / "processed"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/fetch_sources.py"),
            "--config",
            str(TEST_CONFIG),
            "--raw-dir",
            str(raw_dir),
        ],
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/build_names.py"),
            "--config",
            str(TEST_CONFIG),
            "--raw-dir",
            str(raw_dir),
            "--out-dir",
            str(out_dir),
        ],
        check=True,
    )
    return (
        out_dir / "people_names.csv",
        out_dir / "fictional_character_names.csv",
        out_dir / "disambiguated_names.txt",
    )


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_pipeline_outputs_expected_files(tmp_path: Path) -> None:
    people_csv, fictional_csv, disambiguated = run_pipeline(tmp_path)
    assert people_csv.exists()
    assert fictional_csv.exists()
    assert disambiguated.exists()


def test_schema_and_romanization(tmp_path: Path) -> None:
    people_csv, fictional_csv, _ = run_pipeline(tmp_path)
    people_rows = read_csv(people_csv)
    fictional_rows = read_csv(fictional_csv)
    all_rows = people_rows + fictional_rows

    expected_fields = {
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
    }
    assert set(all_rows[0].keys()) == expected_fields

    shinzo = next(r for r in people_rows if r["name"] == "Shinzo Abe")
    assert shinzo["romanized"] == "true"
    assert shinzo["description"] == "Prime Minister of Japan"

    japanese_alias = next(r for r in people_rows if r["name"] == "安倍 晋三")
    assert japanese_alias["romanized"] == "false"

    sherlock_jp = next(r for r in fictional_rows if r["name"] == "シャーロック・ホームズ")
    assert sherlock_jp["romanized"] == "false"
    assert sherlock_jp["description"] == "fictional detective created by Arthur Conan Doyle"


def test_dedup_and_dual_type_behavior(tmp_path: Path) -> None:
    people_csv, fictional_csv, _ = run_pipeline(tmp_path)
    people_rows = read_csv(people_csv)
    fictional_rows = read_csv(fictional_csv)

    # Single-character noise row should be dropped.
    assert not any(r["name"] == "X" for r in people_rows)

    # Dual-typed input produces one person row and one fictional row.
    assert any(r["name"] == "Douglas Adams" for r in people_rows)
    assert any(r["name"] == "Douglas Adams" for r in fictional_rows)

    # Alias rows are preserved with alias_of pointer.
    einstein_alias = next(r for r in people_rows if r["name"] == "Einstein")
    assert einstein_alias["alias_of"] == "Albert Einstein"


def test_disambiguated_names_file(tmp_path: Path) -> None:
    _, _, disambiguated = run_pipeline(tmp_path)
    lines = disambiguated.read_text(encoding="utf-8").splitlines()

    assert "Shinzo Abe" in lines
    assert "Eumedes, son of Melas" in lines
    assert "Eumedes, herald of Priam" in lines
    assert "Eumedes, mythical son of Dolon" in lines
    assert "Eumedes" not in lines
