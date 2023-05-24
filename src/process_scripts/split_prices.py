"""
File Splitting Script
------------------

Script that uses the FileSplitter to split the data into their fuel types and save them in separate files, keeping station and timestamp indices.

- Currently applying this on the level of stratified prices, but before creating equidistant timestamps.
- In a future approach this should probably happen before the first resampling process.
"""
from src.process_files import FileSplitter

from pathlib import Path
from src.config.paths import PROCESSED_PRICES

split_dir = Path(PROCESSED_PRICES / '..' / 'split_prices')
print(f"Splitting prices from {PROCESSED_PRICES}")
print(f"Saving them to {split_dir}")

split = ['diesel', 'e5', 'e10']
splitter = FileSplitter(PROCESSED_PRICES, split_dir, split)
splitter.process_directory()