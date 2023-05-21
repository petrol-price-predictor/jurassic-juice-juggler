import pandas as pd
import datetime as dt

from . import fileutils
from . import process_prices
from . import process_stations

from pathlib import Path
from tqdm import tqdm

class FileProcessor:
    def __init__(self, directory, target_directory, subset=None, subset_index=None):
        self.directory = Path(directory)
        self.target_directory = Path(target_directory)
        self.subset = (subset_index, set(subset))
        self.last_processed = pd.DataFrame()
        self.metadata = pd.DataFrame()
        self.error_files = []


    def process_directory(self):

        # use pathlib to generate a sorted generator expression of subdirectories (1 level)
        subdirectories = sorted(d for d in self.directory.iterdir() if d.is_dir())

        # iterate through the subdirectories, use a tqdm-wrapper to keep track of the progress
        for subdir in tqdm(subdirectories, desc="Processing directories"):
            files = list(fileutils.get_files(subdir))
            with tqdm(total=len(files), desc=f"Processing files in {subdir.name}") as pbar:
                for file in files:
                    try:
                    # Process and save each file
                        self.process_file(file)
                        self.save_to_file(self.last_processed, file)
                    except Exception as e:
                        # If the processing goes somehow wrong, skip the file, raise an error and safe which file wasn't processed
                        print(f"An error occurred processing file {file}: {str(e)}")
                        self.error_files.append(file)
                    pbar.set_postfix_str(f"Current file: {file}", refresh=True)
                    pbar.update()

        
    def process_file(self, file):

        # read the file into a DataFrame and reduce it to the desired subset
        data = pd.read_csv(file)
        if self.subset:
            data = data[data[self.subset[0]].isin(self.subset[1])]

        # process the DataFrame. process_data is a method on the Instance Variables
        self.process_data(data)

        # APPEND STUFF TO self.metadata HERE
        # file_metadata = self.update_metadata(self.last_processed)
        # self.update_metadata(file_metadata)


    def save_to_file(self, data, file):

        # file is required here only to create the new relative Path, but the file itself is not used
        relative_path = file.relative_to(self.directory)
        target = self.target_directory / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(target)


    def process_data(self, data):
        raise NotImplementedError("Subclasses must implement this method")

    def update_metadata(self):
        raise NotImplementedError("Subclasses must implement this method")
    

    def meta_dict(self):
        return {
            'prices_metadata': self.metadata,
        }

    def save_metadata(self, meta_dir, suffix=None):

        if not suffix:
            suffix = ''
        for file_name, data in self.meta_dict().items():
            file_path = Path(meta_dir / f'{file_name}{suffix}.csv')
            fileutils.save_without_overwrite(data, file_path)

class RawPriceProcessor(FileProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_closing_prices = pd.DataFrame()
        self.closing_prices = pd.DataFrame()
    
    def process_data(self, data):

        self.last_processed = process_prices.process_data(data, self.last_closing_prices)
        self.last_closing_prices = process_prices.get_closing_prices(self.last_processed)
        self.update_closing_prices()
        self.update_metadata()

    def update_metadata(self):
        meta = process_prices.get_metadata(self.last_processed)
        self.metadata = pd.concat([self.metadata, meta], ignore_index=True)

    def meta_dict(self):
        metadata = super().meta_dict()
        metadata['closing_prices'] = self.closing_prices
        return metadata
    
    def update_closing_prices(self):

        # Add most recent closing prices to the closing_prices
        self.closing_prices = pd.concat([self.closing_prices, self.last_closing_prices], axis=0)
        # This method is currently not very necessary but can later be extended to check for duplicate data    
    
class StationProcessor(FileProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_data(self, file):
        return process_stations.process_data(file)

    def update_metadata(self, data):
        return process_stations.update_metadata(data)


if __name__ == '__main__':

    from .config.paths import PRICES_DIR, PROCESSED_PRICES
    from .config.paths import STATIONS_DIR, PROCESSED_STATIONS
    from .config.paths import META_DIR, SAMPLE_DIR

    dus_stations_data = pd.read_csv(SAMPLE_DIR / 'stations' / 'stations_dus_plus.csv')
    dus_stations = dus_stations_data.uuid

    print(PRICES_DIR)
    processor = RawPriceProcessor(PRICES_DIR, PROCESSED_PRICES, subset=dus_stations, subset_index='station_uuid')
    processor.process_directory()
    processor.save_metadata(META_DIR)

    print("The following files caused errors:")
    for error_file in processor.error_files:
        print(error_file)
