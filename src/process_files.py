import pandas as pd
import datetime as dt

from . import fileutils
from . import process_prices
from . import process_stations

from .config.paths import ROOT_DIR

from pathlib import Path
import random
from tqdm import tqdm

class FileProcessor:
    def __init__(self, directory, target_directory, subset=None, subset_column=None, subset_df_column=None):
        self.directory = Path(directory)
        self.target_directory = Path(target_directory)
        self.last_processed = pd.DataFrame()
        self.metadata = pd.DataFrame()
        self.error_files = []
        self.set_subset(subset, subset_column, subset_df_column)


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


    def list_files(self):

        files = list(fileutils.get_files(self.directory))
        print(f'{self.directory.relative_to(ROOT_DIR)} contains {len(files)} files.')
        for file in files:
            print(file.relative_to(ROOT_DIR))


    def get_sample(self, suffix='csv', random_state=None):

        random.seed(random_state)
        files = list(Path(self.directory).rglob(f'*.{suffix}'))
        self.sample = pd.read_csv(random.choice(files))
        self.sample = self.get_subset(self.sample)
        return self.sample
    
    
    def set_subset(self, subset, subset_column, subset_df_column=None):
            
            if subset is None:
                self.subset = None

            elif isinstance(subset, pd.DataFrame):
                if isinstance(subset_df_column, str):
                    self.subset = {subset_column: set(subset[subset_df_column])}
                else:
                    raise ValueError("DataFrame requires a column to be specified to be interpreted as pd.Series or use pd.Series as an argument")
            
            elif isinstance(subset, (list, set, tuple, pd.Series)):
                if isinstance(subset_column, str):
                    self.subset = {subset_column: set(subset)}
                else:
                    raise ValueError("subset_column must be specified and must be a string.")
                
            else:
                raise ValueError("Subset needs to be of type list, set, tuple, pd.Series or pd.DataFrame")
        
        
    def get_subset(self, data):
        
        if self.subset is None:
            return data

        for column, values in self.subset.items():
            data = data[data[column].isin(values)]
        return data

        
    def process_file(self, file):

        # read the file into a DataFrame and reduce it to the desired subset
        data = pd.read_csv(Path(file).resolve())
        data = self.get_subset(data)

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
        new_closing_prices = process_prices.get_closing_prices(self.last_processed)
        if self.last_closing_prices.empty:
            self.last_closing_prices = new_closing_prices
        else:
            self.last_closing_prices = new_closing_prices.combine_first(self.last_closing_prices)
        self.update_closing_prices()
        self.update_metadata()

        # returning DataFrame so the method can also be called to directly transform a DataFrame.
        return self.last_processed

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




class PriceProcessor(FileProcessor):

    def __init__(self, directory, target_directory, method=None, method_kwargs={}, *args, **kwargs):

        super().__init__(directory, target_directory, *args, **kwargs)
        self.predefined_methods = process_prices.get_methods()
        self.set_method(method, **method_kwargs)


    def set_method(self, method, **kwargs):
        if method is None:
            self.method = None
        elif type(method) == str:
            if method not in self.predefined_methods:
                raise ValueError(f"{method} is not a predefined method.")
            else:
                self.method = self.predefined_methods[method]
        elif callable(method):
            self.method = method
        else:
            raise ValueError("Passed object is not a function or a predefined method")

        self.method_kwargs = kwargs


    def process_data(self, data: pd.DataFrame, method=None, **kwargs):
        if method:
            self.set_method(method, **kwargs)
        if self.method:
            self.last_processed = self.method(data, **self.method_kwargs)
        else:
            raise ValueError("The method process_data requires a method to be set, but None was given.")
        
        # returning DataFrame so the method can also be called to directly transform a DataFrame.
        return self.last_processed

    def update_metadata(self):
        raise NotImplementedError("Subclasses must implement this method")

    


    
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
    processor = RawPriceProcessor(PRICES_DIR, PROCESSED_PRICES, subset=dus_stations, subset_column='station_uuid')
    processor.process_directory()
    processor.save_metadata(META_DIR)

    print("The following files caused errors:")
    for error_file in processor.error_files:
        print(error_file)
