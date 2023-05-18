import pandas as pd

from . import fileutils
from . import process_prices
from . import process_stations

class FileProcessor:
    def __init__(self):
        self.metadata = pd.DataFrame()

    def process_file(self, file):
        processed_data = self.process_data(file)
        file_metadata = self.collect_metadata(processed_data)
        self.update_metadata(file_metadata)

    def process_data(self, file):
        raise NotImplementedError("Subclasses must implement this method")

    def collect_metadata(self, data):
        raise NotImplementedError("Subclasses must implement this method")

    def update_metadata(self, file_metadata):
        self.metadata = self.metadata.append(file_metadata, ignore_index=True)

    def process_directory(self, directory):
        for file in fileutils.get_files(directory):
            self.process_file(file)

    def save_metadata(self, metadata_file):
        self.metadata.to_csv(metadata_file, index=False)


class PriceProcessor(FileProcessor):
    def __init__(self):
        super().__init__()
        self.closing_prices = pd.DataFrame()

    def process_csv(self, file):
        return process_prices.process_csv(file)

    def collect_metadata(self, data):
        return process_prices.collect_metadata(data)
    
    def update_closing_prices(self, data):
        pass
    
    def add_closing_prices(self, data):
        return process_prices.add_closing_prices(data)
    
    
class StationProcessor(FileProcessor):
    def process_csv(self, file):
        return process_stations.process_csv(file)

    def collect_metadata(self, data):
        return process_stations.collect_metadata(data)


if __name__ == '__main__':
    pass
