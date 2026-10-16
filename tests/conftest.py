# Tambahkan folder producer dan consumer ke sys.path supaya modulnya bisa di-import langsung oleh tests
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for folder in ("producer", "consumer"):
    sys.path.insert(0, str(ROOT / folder))
