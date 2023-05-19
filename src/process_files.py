import pandas as pd

from . import fileutils
from . import process_prices
from . import process_stations

from pathlib import Path
from tqdm import tqdm

class FileProcessor:
    def __init__(self):
        self.last_processed = pd.DataFrame()
        self.metadata = pd.DataFrame()
        self.error_files = []


    def process_directory(self, directory, target_directory):
        subdirectories = [d for d in directory.iterdir() if d.is_dir()]
        
        for subdir in tqdm(subdirectories, desc="Processing directories"):
            files = list(fileutils.get_files(subdir))
            with tqdm(total=len(files), desc=f"Processing files in {subdir.name}") as pbar:
                for file in files:
                    try:
                        self.process_file(file)
                        self.save_to_file(self.last_processed, file, target_directory)
                    except Exception as e:
                        print(f"An error occurred processing file {file}: {str(e)}")
                        self.error_files.append(file)
                    pbar.set_postfix_str(f"Current file: {file}", refresh=True)
                    pbar.update()

        
    def process_file(self, file):
        # take a file, process it
        self.last_processed = self.process_data(file)

        # file_metadata = self.collect_metadata(self.last_processed)
        # self.update_metadata(file_metadata)


    def save_to_file(self, data, file, target_directory):

        relative_path = file.relative_to(PRICES_DIR)
        target = Path(target_directory / relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(target)


    def process_data(self, file):
        raise NotImplementedError("Subclasses must implement this method")

    def collect_metadata(self, data):
        raise NotImplementedError("Subclasses must implement this method")
    

    def update_metadata(self, file_metadata):
        self.metadata = self.metadata.append(file_metadata, ignore_index=False)

    def save_metadata(self, data, dir, suffix=''):
        # NEED TO CHANGE THIS AND CALL MANUALLY
        dir = Path(dir)
        if not suffix:
            suffix = dir.name
        filename = f'{suffix}_metadata'
        data.to_csv(Path(dir / filename), index=True)

class PriceProcessor(FileProcessor):
    def __init__(self):
        super().__init__()
        self.last_closing_prices = pd.DataFrame()
        self.closing_prices = pd.DataFrame()

    def process_directory(self, directory, target_directory):

        super().process_directory(directory, target_directory)
        # self.save_metadata(self.closing_prices, directory, target_directory, suffix='closing_prices')
    
    def process_file(self, file):
        super().process_file(file)

        self.last_closing_prices = process_prices.get_closing_prices(self.last_processed)
        self.update_closing_prices(self.last_closing_prices)

    def process_data(self, file):
        return process_prices.process_csv(file)

    def collect_metadata(self, data):
        return process_prices.collect_metadata(data)
    
    def update_closing_prices(self, data):
        pass
    
    
class StationProcessor(FileProcessor):
    def process_data(self, file):
        return process_stations.process_csv(file)

    def collect_metadata(self, data):
        return process_stations.collect_metadata(data)


if __name__ == '__main__':

    from .config.paths import PRICES_DIR, PROCESSED_PRICES
    from .config.paths import STATIONS_DIR, PROCESSED_STATIONS
    from .config.paths import META_DIR

    print(PRICES_DIR)
    processor = PriceProcessor()
    processor.process_directory(PRICES_DIR, PROCESSED_PRICES)

    print("The following files caused errors:")
    for error_file in processor.error_files:
        print(error_file)
