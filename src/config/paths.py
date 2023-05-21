from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

PRICES_DIR = ROOT_DIR / 'data' / 'prices'
STATIONS_DIR = ROOT_DIR / 'data' / 'stations'
SAMPLE_DIR = ROOT_DIR / 'data' / 'sample'

META_DIR = ROOT_DIR / 'data_processed' / 'meta'
PROCESSED_DIR = ROOT_DIR / 'data_processed'
PROCESSED_PRICES = ROOT_DIR / 'data_processed' / 'prices'
PROCESSED_STATIONS = ROOT_DIR / 'data_processed' / 'stations'
