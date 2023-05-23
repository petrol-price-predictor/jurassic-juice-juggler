from src.process_files import FileSplitter

from pathlib import Path
from src.config.paths import PROCESSED_PRICES

split_dir = Path(PROCESSED_PRICES / '..' / 'split_prices')
print(f"Splitting prices from {PROCESSED_PRICES}")
print(f"Saving them to {split_dir}")

split = ['diesel', 'e5', 'e10']
splitter = FileSplitter(PROCESSED_PRICES, split_dir, split)
splitter.process_directory()