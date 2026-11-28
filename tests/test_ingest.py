from datetime import date

from scripts import cfpb_ingest


def test_watermark_roundtrip(tmp_path, monkeypatch):
    # 1. point the state file at a temporary folder
    # 2. with no file, load_watermark return none
    # 3. save the date, load it back, check it is the same date
    monkeypatch.setattr(cfpb_ingest, "STATE_FILE", tmp_path / "state.json")

    assert cfpb_ingest.load_watermark() is None

    cfpb_ingest.save_watermark(date(2026, 11, 1))

    assert cfpb_ingest.load_watermark() == date(2026, 11, 1)
