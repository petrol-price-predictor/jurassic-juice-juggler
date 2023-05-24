"""
File Merger Script
------------------

Script that vertically merges all files contained in /resampled_prices/, manually split by sub-folders into a single panel for the DUS subset of stations
"""
from src.process_files import FileMerger

from pathlib import Path
from src.config.paths import ROOT_DIR


resample_dir = Path(ROOT_DIR / 'resampled_prices')

print(f"Merging prices from {Path(resample_dir)}")
print(f"Saving them to {Path(resample_dir / 'merged')}")

fuels = ['diesel', 'e5', 'e10']

for fuel in fuels:
    source = Path(resample_dir / fuel)
    target = Path(resample_dir)
    processor = FileMerger(source, target)

    processor.process_directory()
    processor.save_to_file(processor.merged_data)