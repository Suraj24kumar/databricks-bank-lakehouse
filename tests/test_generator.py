import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bank_lakehouse.generator import generate_day  # noqa: E402


def _rows(root, feed, label):
    with open(Path(root) / feed / f"dt={label}" / f"{feed}_{label}.csv") as fh:
        return list(csv.DictReader(fh))


def test_same_day_is_byte_identical(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate_day(3, a)
    generate_day(3, b)
    for feed in ("customers", "accounts", "transactions"):
        fa = next((a / feed).rglob("*.csv")).read_bytes()
        fb = next((b / feed).rglob("*.csv")).read_bytes()
        assert fa == fb


def test_day0_has_initial_customers_only_upserts(tmp_path):
    m = generate_day(0, tmp_path)
    rows = _rows(tmp_path, "customers", m["date"])
    assert len(rows) == 300 and {r["op"] for r in rows} == {"UPSERT"}


def test_planted_faults_match_manifest(tmp_path):
    m = generate_day(2, tmp_path)
    tx = _rows(tmp_path, "transactions", m["date"])
    assert sum(1 for r in tx if r["account_id"] == "") == m["null_account"]
    assert sum(1 for r in tx if float(r["amount"]) < 0) == m["negative_amount"]
    assert sum(1 for r in tx if r["channel"] == "FAX") == m["unknown_channel"]
    assert sum(1 for r in tx if r["txn_ts"].startswith("2026-13")) == m["bad_timestamp"]
    # replays are yesterday's transactions delivered again, i.e. duplicates ACROSS two landing files
    generate_day(1, tmp_path)
    prev_ids = {r["txn_id"] for r in _rows(tmp_path, "transactions", "2026-01-02")}
    assert sum(1 for r in tx if r["txn_id"] in prev_ids) == m["replayed_duplicates"]


def test_later_day_has_changes_and_a_delete_on_day5(tmp_path):
    generate_day(5, tmp_path)
    rows = _rows(tmp_path, "customers", "2026-01-06")
    assert any(r["op"] == "DELETE" for r in rows)
    assert any(r["op"] == "UPSERT" for r in rows)
