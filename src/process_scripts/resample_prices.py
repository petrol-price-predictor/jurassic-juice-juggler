from src.process_files import PriceProcessor

from pathlib import Path
from src.config.paths import PROCESSED_PRICES, ROOT_DIR
from src import process_prices



split_dir = Path(PROCESSED_PRICES / '..' / 'split_prices')
resample_dir = Path(ROOT_DIR / 'resampled_prices')

print(f"Splitting prices from {split_dir}")
print(f"Saving them to {resample_dir}")

fuels = ['diesel', 'e5', 'e10']

for fuel in fuels:
    source = Path(split_dir / fuel)
    target = Path(resample_dir / fuel)
    processor = PriceProcessor(source, target)

    agg_dict = {
        fuel: 'mean',
        f'{fuel}_is_selling': 'max',
        'total_changes': 'count'
        }

    processor.set_method(process_prices.resample_timestamps, agg_dict, freq='H')
    processor.process_directory()
