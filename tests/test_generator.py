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


def test_round_trip_customers_have_home_away_away_home(tmp_path):
    m = generate_day(2, tmp_path)
    tx = [r for r in _rows(tmp_path, "transactions", m["date"]) if r["account_id"] and not r["txn_ts"].startswith("2026-13")]
    assert len(m["round_trip_customers"]) == 5
    checked = 0
    for cid in m["round_trip_customers"]:
        rows = sorted((r for r in tx if r["customer_id"] == cid), key=lambda r: r["txn_id"])  # trip rows are created first each day
        cities = [r["txn_city"] for r in rows][:4]
        if len(cities) == 4:  # skip a customer whose trip rows were hit by a planted fault
            assert cities[0] == cities[3] and cities[1] == cities[2] and cities[0] != cities[1]
            checked += 1
    assert checked >= 3


def test_txn_city_is_mostly_home_but_sometimes_away(tmp_path):
    m = generate_day(0, tmp_path)
    cust = {r["customer_id"]: r["city"] for r in _rows(tmp_path, "customers", m["date"])}
    acc = {r["account_id"]: r["customer_id"] for r in _rows(tmp_path, "accounts", m["date"])}
    tx = [r for r in _rows(tmp_path, "transactions", m["date"]) if r["account_id"] in acc]
    away = sum(1 for r in tx if r["txn_city"] != cust[acc[r["account_id"]]])
    assert 0 < away < len(tx) * 0.25
