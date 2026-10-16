import json
from datetime import datetime, timedelta, timezone

import pytest

import landing_consumer as consumer
from event_schema import EVENT_TYPES, to_iso

ENV_NAMES = (
    "LANDING_DIR",
    "BATCH_SIZE",
    "BATCH_FLUSH_SECONDS",
    "CONSUMER_METRICS_PORT",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_USER",
    "RABBITMQ_PASSWORD",
    "RABBITMQ_QUEUE",
)


# Buat isi pesan RabbitMQ (bytes) dari event valid, field bisa ditimpa lewat argumen
def make_body(**overrides):
    event = {
        "event_id": "e1",
        "event_type": "payment",
        "event_time": "2017-10-02T10:56:35.001Z",
    }
    event.update(overrides)
    return json.dumps(event).encode("utf-8")


# Kosongkan semua variabel lingkungan yang dibaca consumer supaya test tidak terpengaruh shell lokal
@pytest.fixture
def clean_env(monkeypatch):
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


# Channel palsu yang mencatat ack dan jumlah file landing pada saat ack terjadi
class FakeChannel:
    def __init__(self, landing_dir):
        self.landing_dir = landing_dir
        self.acks = []
        self.files_at_ack = []

    # Catat pemanggilan ack beserta jumlah file landing saat itu
    def basic_ack(self, delivery_tag, multiple=False):
        self.acks.append((delivery_tag, multiple))
        self.files_at_ack.append(len(list(self.landing_dir.glob("orders-*.json"))))


# Event dengan envelope lengkap lolos dan dikembalikan apa adanya
def test_valid_message_passes():
    event, reason = consumer.inspect_message(make_body())
    assert reason is None
    assert event["event_id"] == "e1"


# JSON rusak, termasuk byte yang bukan UTF-8, ditolak dengan alasan invalid_json
@pytest.mark.parametrize("body", [b"{bukan json", b"", b"\xff\xfe"])
def test_invalid_json_is_rejected(body):
    assert consumer.inspect_message(body) == (None, "invalid_json")


# JSON valid yang bukan objek ditolak dengan alasan not_object
@pytest.mark.parametrize("body", [b"[1, 2]", b'"teks"', b"123", b"null"])
def test_non_object_is_rejected(body):
    assert consumer.inspect_message(body) == (None, "not_object")


# event_type yang tidak dikenal atau tidak ada ditolak
def test_unknown_or_missing_event_type_is_rejected():
    assert consumer.inspect_message(make_body(event_type="refund")) == (None, "unknown_event_type")
    body = json.dumps({"event_id": "e1", "event_time": "2017-10-02T10:56:35.001Z"}).encode("utf-8")
    assert consumer.inspect_message(body) == (None, "unknown_event_type")


# event_id kosong atau null ditolak
@pytest.mark.parametrize("value", [None, ""])
def test_missing_event_id_is_rejected(value):
    assert consumer.inspect_message(make_body(event_id=value)) == (None, "missing_event_id")


# event_time kosong atau null ditolak
@pytest.mark.parametrize("value", [None, ""])
def test_missing_event_time_is_rejected(value):
    assert consumer.inspect_message(make_body(event_time=value)) == (None, "missing_event_time")


# Cek envelope tidak memeriksa payload (tugas validasi Spark), jadi event dengan field bisnis rusak tetap lolos
def test_envelope_check_does_not_validate_payload():
    event, reason = consumer.inspect_message(make_body(order_id=None, payment_value=-5))
    assert reason is None
    assert event["payment_value"] == -5


# Daftar tipe event consumer harus sama dengan producer supaya tidak ada drift
def test_known_event_types_match_producer():
    assert set(consumer.KNOWN_EVENT_TYPES) == set(EVENT_TYPES)


# Nilai kosong atau rusak menghasilkan None, bukan error
@pytest.mark.parametrize("value", [None, "", "bukan waktu"])
def test_seconds_since_returns_none_for_bad_values(value):
    assert consumer.seconds_since(value) is None


# Waktu yang baru lewat 5 detik terukur sekitar 5 detik, dengan toleransi untuk mesin yang lambat
def test_seconds_since_measures_recent_time():
    moment = to_iso(datetime.now(timezone.utc) - timedelta(seconds=5))
    assert 4 <= consumer.seconds_since(moment) < 60


# Waktu tanpa zona diperlakukan sebagai UTC
def test_seconds_since_treats_naive_time_as_utc():
    assert consumer.seconds_since("2017-10-02T10:56:33") > 1e8


# write_batch menulis JSON lines berurutan dan tidak meninggalkan file sementara
def test_write_batch_creates_json_lines_without_temp_files(tmp_path):
    landing = tmp_path / "landing"
    events = [{"event_id": f"e{index}"} for index in range(3)]
    path = consumer.write_batch(landing, events)
    assert path.name.startswith("orders-")
    assert path.suffix == ".json"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["event_id"] for line in lines] == ["e0", "e1", "e2"]
    assert list(landing.glob(".tmp-*")) == []


# Dua batch berturut-turut tidak boleh memakai nama file yang sama
def test_write_batch_uses_unique_names(tmp_path):
    first = consumer.write_batch(tmp_path, [{"event_id": "a"}])
    second = consumer.write_batch(tmp_path, [{"event_id": "b"}])
    assert first != second


# Ack hanya boleh terjadi setelah file landing tertulis (at-least-once), dengan multiple=True pada delivery tag terakhir
def test_flush_buffer_acks_only_after_file_is_written(tmp_path):
    landing = tmp_path / "landing"
    channel = FakeChannel(landing)
    buffer = [
        (1, json.loads(make_body(event_id="e1"))),
        (2, json.loads(make_body(event_id="e2"))),
        (3, json.loads(make_body(event_id="e3"))),
    ]
    consumer.flush_buffer(channel, buffer, landing)
    assert channel.acks == [(3, True)]
    assert channel.files_at_ack == [1]
    assert buffer == []
    files = list(landing.glob("orders-*.json"))
    assert len(files) == 1
    assert len(files[0].read_text(encoding="utf-8").splitlines()) == 3


# Konfigurasi bawaan dipakai bila tidak ada variabel lingkungan
def test_load_config_defaults(clean_env):
    config = consumer.load_config()
    assert config["batch_size"] == 500
    assert config["flush_seconds"] == 5
    assert config["queue"] == "orders.events.queue"
    assert config["landing_dir"] == consumer.PROJECT_ROOT / "data" / "landing"


# Nilai variabel yang hanya berisi spasi dianggap tidak diisi
def test_load_config_blank_value_falls_back_to_default(clean_env):
    clean_env.setenv("BATCH_SIZE", "   ")
    assert consumer.load_config()["batch_size"] == 500


# Path landing absolut dipakai apa adanya
def test_load_config_keeps_absolute_landing_dir(clean_env, tmp_path):
    clean_env.setenv("LANDING_DIR", str(tmp_path))
    assert consumer.load_config()["landing_dir"] == tmp_path


# Nilai batch yang nol, negatif, atau bukan angka ditolak dengan ValueError
@pytest.mark.parametrize(
    "name,value",
    [("BATCH_SIZE", "0"), ("BATCH_FLUSH_SECONDS", "-1"), ("BATCH_SIZE", "abc")],
)
def test_load_config_rejects_invalid_values(clean_env, name, value):
    clean_env.setenv(name, value)
    with pytest.raises(ValueError):
        consumer.load_config()
