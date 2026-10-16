# Pola replay: time_compression (delay dari selisih event_time) dan fixed_rate (event per detik tetap)
import time
from datetime import datetime, timezone

from event_schema import parse_iso, to_iso

REPLAY_MODES = ("time_compression", "fixed_rate")


# Hitung delay antar dua event: selisih event_time dibagi speed_factor, dibatasi max_sleep_seconds
def compute_delay(previous_time, current_time, speed_factor, max_sleep_seconds):
    if previous_time is None or current_time is None:
        return 0.0
    gap_seconds = (current_time - previous_time).total_seconds()
    if gap_seconds <= 0:
        return 0.0
    return min(gap_seconds / speed_factor, max_sleep_seconds)


# Parse event_time dengan aman, kembalikan None kalau kosong atau rusak (event rusak tetap harus terkirim)
def safe_parse_time(value):
    try:
        return parse_iso(value)
    except (TypeError, ValueError, AttributeError):
        return None


# Tempel publish_time (waktu kirim sekarang) ke salinan event
def stamp_publish_time(event):
    stamped = dict(event)
    stamped["publish_time"] = to_iso(datetime.now(timezone.utc))
    return stamped


# Kirim event satu per satu lewat publish_fn dengan jeda sesuai mode, kembalikan jumlah event terkirim
def replay_events(
    events,
    publish_fn,
    mode,
    speed_factor,
    max_sleep_seconds,
    events_per_second,
    sleep_fn=time.sleep,
):
    if mode not in REPLAY_MODES:
        raise ValueError(f"REPLAY_MODE harus salah satu dari {REPLAY_MODES}, bukan {mode}")
    if mode == "time_compression" and speed_factor <= 0:
        raise ValueError("SPEED_FACTOR harus lebih besar dari 0")
    if mode == "fixed_rate" and events_per_second <= 0:
        raise ValueError("EVENTS_PER_SECOND harus lebih besar dari 0")

    sent = 0
    previous_time = None
    started = time.perf_counter()

    for event in events:
        if mode == "time_compression":
            current_time = safe_parse_time(event.get("event_time"))
            delay = compute_delay(previous_time, current_time, speed_factor, max_sleep_seconds)
            if current_time is not None:
                previous_time = current_time
            if delay > 0:
                sleep_fn(delay)
        else:
            due = started + sent / events_per_second
            wait = due - time.perf_counter()
            if wait > 0:
                sleep_fn(wait)

        publish_fn(stamp_publish_time(event))
        sent += 1

    return sent
