from datetime import datetime, timedelta, timezone

import pytest

from event_schema import parse_iso, to_iso
from replay import compute_delay, replay_events, safe_parse_time, stamp_publish_time

BASE = datetime(2017, 10, 2, 10, 0, 0, tzinfo=timezone.utc)


# Buat event minimal dengan event_time = BASE + offset detik
def make_timed_event(offset_seconds, event_type="order_created"):
    return {
        "event_id": f"e{offset_seconds}",
        "event_type": event_type,
        "event_time": to_iso(BASE + timedelta(seconds=offset_seconds)),
    }


# Jalankan replay dengan sleep palsu, kembalikan (jumlah terkirim, daftar delay, event terkirim)
def run_replay(events, **kwargs):
    published = []
    sleeps = []
    params = {
        "mode": "time_compression",
        "speed_factor": 3600,
        "max_sleep_seconds": 5,
        "events_per_second": 50,
    }
    params.update(kwargs)
    sent = replay_events(events, published.append, sleep_fn=sleeps.append, **params)
    return sent, sleeps, published


# Delay = selisih waktu dibagi speed_factor
def test_compute_delay_divides_gap_by_speed_factor():
    assert compute_delay(BASE, BASE + timedelta(seconds=7200), 3600, 10) == 2.0


# Delay dibatasi max_sleep_seconds untuk gap besar
def test_compute_delay_is_capped():
    assert compute_delay(BASE, BASE + timedelta(days=30), 3600, 1) == 1


# Selisih nol, negatif, atau waktu tidak tersedia tidak menghasilkan delay
def test_compute_delay_is_zero_for_non_positive_or_missing():
    assert compute_delay(BASE, BASE, 3600, 1) == 0.0
    assert compute_delay(BASE, BASE - timedelta(seconds=10), 3600, 1) == 0.0
    assert compute_delay(None, BASE, 3600, 1) == 0.0
    assert compute_delay(BASE, None, 3600, 1) == 0.0


# event_time rusak menghasilkan None, bukan error
def test_safe_parse_time_returns_none_for_bad_values():
    assert safe_parse_time(None) is None
    assert safe_parse_time("bukan waktu") is None
    assert safe_parse_time(to_iso(BASE)) == BASE


# publish_time ditempel ke salinan, event asli tidak berubah
def test_stamp_publish_time_does_not_mutate_original():
    original = make_timed_event(0)
    stamped = stamp_publish_time(original)
    assert "publish_time" not in original
    assert parse_iso(stamped["publish_time"]).tzinfo is not None
    assert stamped["event_time"] == original["event_time"]


# time_compression: tidur sesuai selisih waktu, gap besar dipotong, gap nol tidak tidur
def test_time_compression_sleeps_follow_event_gaps():
    events = [make_timed_event(0), make_timed_event(3600), make_timed_event(3600), make_timed_event(363600)]
    sent, sleeps, published = run_replay(events)
    assert sent == 4
    assert sleeps == [1.0, 5.0]
    assert all("publish_time" in event for event in published)


# Event dengan event_time rusak tetap terkirim dan tidak membuat replay gagal
def test_replay_survives_corrupted_event_time():
    events = [make_timed_event(0), {"event_id": "x", "event_type": "payment", "event_time": None}, make_timed_event(60)]
    sent, _, published = run_replay(events)
    assert sent == 3
    assert len(published) == 3


# fixed_rate: tidur mengikuti jadwal event per detik, event pertama langsung terkirim
def test_fixed_rate_schedules_events_by_rate():
    events = [make_timed_event(offset) for offset in range(5)]
    sent, sleeps, _ = run_replay(events, mode="fixed_rate", events_per_second=10)
    assert sent == 5
    assert len(sleeps) == 4
    assert all(value > 0 for value in sleeps)


# Parameter tidak valid harus ditolak sebelum ada event terkirim
def test_invalid_parameters_raise_value_error():
    events = [make_timed_event(0)]
    with pytest.raises(ValueError):
        run_replay(events, mode="turbo")
    with pytest.raises(ValueError):
        run_replay(events, speed_factor=0)
    with pytest.raises(ValueError):
        run_replay(events, mode="fixed_rate", events_per_second=0)
