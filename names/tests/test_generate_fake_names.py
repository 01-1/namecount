from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
TEST_CONFIG = ROOT / "config/fake_names.test.yaml"


def test_fake_name_generation_with_faker(tmp_path: Path) -> None:
    output_path = tmp_path / "fake_names.txt"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/generate_fake_names.py"),
            "--config",
            str(TEST_CONFIG),
            "--output",
            str(output_path),
            "--target-count",
            "40",
            "--seed",
            "42",
        ],
        check=True,
    )

    lines = [line.strip() for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 40
    assert len(set(lines)) == 40
    assert "Shinzo Abe" not in lines


def test_near_match_index_blocks_similar_names() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import generate_fake_names as g

    idx = g.NearMatchIndex(["shinzo abe", "eumedes"], threshold=0.9, max_len_diff=3)
    assert idx.is_near_match("shinzo abee")
    assert not idx.is_near_match("lina varos")
