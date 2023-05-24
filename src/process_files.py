"""
FileProcessor module
--------------------
This module implements the FileProcessor class, including sub-classes. Its main functionality is to process panel-data spread out over many files and directories.

- FileProcessor(): A generic parent-class containing methods to deal with and iterate over directories and subdirectories and process the files within.
- RawPriceProcessor(): A subclass specified to deal with the raw file format imported from the Tankerkönig API.
- FileSplitter(): A subclass specified to horizontally split the files into columns, keeping their indices, and saving them into multiple files.
- FileMerger(): A subclass specified to vertically merge all files within a folder into a single file.
- PriceProcessor(): A subclass that can be used to transform just about any csv file by applying a function or importing a predefined function and then processing an full directory in this manner.

Functions that are specific to the data in this project are imported from src.process and src.price_process to keep this class modular and reusable.

Running this module as __main__ will process all raw data imported from Tankerkönig. Currently can't take any arguments.

IMPORTANT: All files that need to be processed in a specific order like time-series data rely on the files's naming convention to be sortable.
"""
import pandas as pd
import datetime as dt
import time

from . import fileutils
from . import process_prices
from . import process_stations

from .config.paths import ROOT_DIR

from pathlib import Path
import random
from tqdm import tqdm

class FileProcessor:
    """FileProcessor is a parent class that is not supposed to be run by itself. It contains core functionality inherited to and used by all sub-classes.
    
    Args:
        directory (str or Path): directory of files that are to be processed
        target_directory (str or Path): directory to save processed files into. structure of directory will be mirrored.
    
    Methods:
        process_directory(): Process all files contained in directory, provides a progressbar as processing may take a while.
        list_files(): Returns a list of all files that are to be processed
        get_sample(suffix, random_state): Picks a random sample from all files in list_files to work with before processing. can be accessed with self.sample
        set_subset(subset, subset_column, subset_df_column): Will be called automatically on __init__, but can also be called after init to process only a subset.
        get_subset(): Method used mostly internally reducing the current DataFrame to the specified subset when being called.
        process_file(file): Method to load a file into a DataFrame, reduce it a subset if specified, process the data and then save the new file.
        save_to_file(data, file): Method to save a DataFrame in the target_directory with a relative file location as the original file location.
        meta_dict(): Method that contains a dictionary about what meta information is to be stored from each file in an extra metadata DataFrame
        save_metadata(). Saved the metadata stored in self.metadata after calling process_directory()
    """
    def __init__(self, directory, target_directory, subset=None, subset_column=None, subset_df_column=None):
        """On instantiation only stores information about the source directory files and, if already specified, the data subset.

        Args:
            directory (str or Path): directory of files that are to be processed
            target_directory (str or Path): directory to save processed files into. structure of directory will be mirrored.
            subset (iterable or DataFrame, optional): see set_subset()
            subset_column (_type_, optional): see set_subset()
            subset_df_column (_type_, optional): see set_subset()
        """
        self.directory = Path(directory)
        self.target_directory = Path(target_directory)
        self.last_processed = pd.DataFrame()
        self.metadata = pd.DataFrame()
        self.error_files = []
        self.set_subset(subset, subset_column, subset_df_column)


    def process_directory(self):
        """Process all files in self.directory and call process_file() on them"""

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
                    except Exception as e:
                        # If the processing goes somehow wrong, skip the file, raise an error and safe which file wasn't processed
                        print(f"An error occurred processing file {file}: {str(e)}")
                        self.error_files.append(file)
                    pbar.set_postfix_str(f"Current file: {file}", refresh=True)
                    pbar.update()


    def list_files(self):
        """Prints a list of all files that are to be processed and returns it as a list."""

        files = list(fileutils.get_files(self.directory))
        print(f'{self.directory.relative_to(ROOT_DIR)} contains {len(files)} files.')
        for file in files:
            print(file.relative_to(ROOT_DIR))
        return files


    def get_sample(self, suffix: str='csv', random_state:int=None)->pd.DataFrame:
        """Loads a file into self.sample as pd.DataFrame and returns it if required. If a subset is specified, already applies it

        Args:
            suffix (str, optional): file-ending of random sample file. Defaults to 'csv'.
            random_state (int, optional): random seed to reproduce results. Defaults to None.

        Returns:
            pd.DataFrame: randomly chosen file loaded into a DataFrame
        """

        random.seed(random_state)
        files = list(Path(self.directory).rglob(f'*.{suffix}'))
        self.sample = pd.read_csv(random.choice(files))
        self.sample = self.get_subset(self.sample)
        return self.sample
    
    
    def set_subset(self, subset, subset_column: str, subset_df_column: str=None):
        """Filter the original data down to a subset. Currently only applicable to one column. Only defines a subset, makes no processing.

        Args:
            subset (iterable or pd.DataFrame): A list, set, tuple, pd.Series or pd.DataFrame that contains an array specifying a subset.
            subset_column (_type_): Column in the processing file to filter from.
            subset_df_column (str, optional): if subset is a pd.DataFrame then a column must be specified to reduce it to a pd.Series. Defaults to None.

        Raises:
            ValueError: Raises a ValueError if subset is a DataFrame but no column was specified
            ValueError: Raises a ValueError if a subset is specified but the column in the processing file can't be found
            ValueError: Raises a ValueError if the subset is of non-interpretable DataType
        """
            
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
        """Filter the processing DataFrame with the defined subset"""
        
        # checks if a subset was defined
        if self.subset is None:
            return data

        # filters the DataFrame down to its subset
        for column, values in self.subset.items():
            data = data[data[column].isin(values)]
        return data

        
    def process_file(self, file):
        """Default method how to process a file on a file basis. Currently saves no metadata by default. Includes saving a file"""

        # read the file into a DataFrame and reduce it to the desired subset
        data = pd.read_csv(Path(file).resolve())
        data = self.get_subset(data)

        # process the DataFrame. process_data is a method on the Instance Variables
        self.process_data(data)
        self.save_to_file(self.last_processed, file)

        # APPEND STUFF TO self.metadata HERE
        # file_metadata = self.update_metadata(self.last_processed)
        # self.update_metadata(file_metadata)


    def save_to_file(self, data, file):
        """Method to save a file in the specified target_directory. Keeps the originals directory file structure by looking up relative paths."""

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
        """Method to be looked up when creating metadata. Contains which information to extract from each file. Not implemented in this parent class."""
        return {
            'prices_metadata': self.metadata,
        }

    def save_metadata(self, meta_dir, suffix=None):
        """Method to save metadata in self.metadata to a file. Does not overwrite existing metadata files"""

        if not suffix:
            suffix = ''
        for file_name, data in self.meta_dict().items():
            file_path = Path(meta_dir / f'{file_name}{suffix}.csv')
            fileutils.save_without_overwrite(data, file_path)

class RawPriceProcessor(FileProcessor):
    """This subclass' main purpose is to process raw panel-data, currently specific to this projects data-structure but easily adjustable.

    Careful, if no subset is specified this can easily expand a panel DataFrame to very unreasonable DataFrame sizes that Pandas is not layed out for.
    The main problems it deals with are:
    - Carrying over data from one file to another: Time is continuous but data is stored in single files for each day. Some information from the previous day is required to process the next day
    - Irregular timestamps and sparse observations: The panel will be stratified by cross-multiplying all individuals and all timestamps in a panel resulting in one observation per individual per timestamp. 
    - This class will not create equidistant timestamps.
    - transform timezone specific datetime-strings into a correct datetime format.
    - Sort data by individual firstly and by datetime secondly.
    - Stores the last observation for each individual and each file as metadata.
    - Stores average prices for each day as metadata to generate daily data.

    Args:
        FileProcessor (class): This is a sub-class of the FileProcessor-class
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_closing_prices = pd.DataFrame()
        self.closing_prices = pd.DataFrame()
    
    def process_data(self, data):
        """
        Implementing the method not implemented in the parent class on how to process data.
        Imports the specifics from src.process_prices, extracts closing prices and metadata from the transformed panel
        """

        self.last_processed = process_prices.process_data(data, self.last_closing_prices)
        new_closing_prices = process_prices.get_closing_prices(self.last_processed)

        # closing prices is empty on the first iteration so it needs to be treated differently
        if self.last_closing_prices.empty:
            self.last_closing_prices = new_closing_prices
        else:
            self.last_closing_prices = new_closing_prices.combine_first(self.last_closing_prices)

        # update closing prices and metadata
        self.update_closing_prices()
        self.update_metadata()

        # returning DataFrame so the method can also be called to directly transform a DataFrame.
        return self.last_processed

    def update_metadata(self):
        """Method that updates self.metadata with data from the processed DataFrame. Function specifics are imported"""

        meta = process_prices.get_metadata(self.last_processed)
        self.metadata = pd.concat([self.metadata, meta], ignore_index=True)

    def meta_dict(self):
        """Extended version of the metadata dict from the parent class. Required for saving it to a file."""

        metadata = super().meta_dict()
        metadata['closing_prices'] = self.closing_prices
        return metadata
    
    def update_closing_prices(self):
        """Update the closing prices after each file iteration."""

        # Add most recent closing prices to the closing_prices
        self.closing_prices = pd.concat([self.closing_prices, self.last_closing_prices], axis=0)
        # This method is currently not very necessary but can later be extended to check for duplicate data


class FileSplitter(FileProcessor):
    """Subclass to split panel data vertically into new DataFrames with reduced columns and saves them into files.
    The main purpose of this is to split data into files for each fuel type, while keeping station and datetime information in all of them
    Creates a subfolder for each of the splits.

    Args:
        FileProcessor (class): This is a sub-class of the FileProcessor-class
    """

    def __init__(self, directory, target_directory, split: list, *args, **kwargs):
        """
        Args:
            directory (str or Path): directory of files that are to be processed
            target_directory (str or Path): directory to save processed files into. structure of directory will be mirrored.
            split (list): In this version, split can only accept list of like-wise column names.
                          If an element of split is 'diesel' then the resulting files' DataFrame will only contain columns with 'diesel' in the column name.
            Creates one file per element in split for each file being processed
        """
        super().__init__(directory, target_directory, *args, **kwargs)
        self.last_processed = {}
        self.split = split

    def save_to_file(self, data, file):
        """Modified version of save_to_file from FileProcessor to accommodate for split-folders."""

        # file is required here only to create the new relative Path, but the file itself is not used
        for key, data in data.items():
            relative_path = file.relative_to(self.directory)
            target = self.target_directory / key / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(target)

    def process_data(self, data):
        """Set and keep panel indices in each of the dataframes while splitting the remainder of columns into separate DataFrames"""

        self.last_processed = process_prices.split_panel(data, self.split)
        return self.last_processed

    def update_metadata(self):
        """No purpose in this class"""

        raise NotImplementedError("Not implemented for this subclass")
    
    
class FileMerger(FileProcessor):
    """Subclass to vertically merge all csv files in a directory into a single file.
       Current implementation returns a file that is sorted by individuals and date with very specific column names.
       Needs adjustment in the future.
    """

    def __init__(self, directory, target_directory, *args, **kwargs):
        super().__init__(directory, target_directory, *args, **kwargs)
        self.merged_data = pd.DataFrame()
        self.data_list = []

    def process_directory(self):
        """Modified version of the parent-class' version that, after loading all files into memory, merges and sorts them.
           Merging and sorting is implemented in src.process_prices and can be either replaced or adjusted.
           Can work with subsets like any of the other child-classes.
           All data that is to be merged needs to fit into memory.
        """

        super().process_directory()
        start_time = time.time()
        self.merged_data = process_prices.merge_sort_index(self.data_list)
        end_time = time.time()
        tqdm.write(f'Merged {len(self.merged_data)} rows in {end_time - start_time} seconds.')

    def process_file(self, file):
        """Modified implementation of process_file that, unlike in all other subclasses, does not save the file immediately after processing"""

        # read the file into a DataFrame and reduce it to the desired subset
        data = pd.read_csv(Path(file).resolve())
        data = self.get_subset(data)

        # process the DataFrame. process_data is a method on the Instance Variables.
        self.process_data(data)
        # this subclass does not save the file immediately


    def save_to_file(self, data, dir=None):
        "Modified implementation of save_to_file with a different target_directory and naming convention"

        # Saving merged file(s) into a parallel /merged/ directory with the original directories name as filename
        f'{self.directory.name}.csv'
        if not dir:
            dir = Path(self.target_directory / 'merged')
        dir.mkdir(parents=True, exist_ok=True)
        dir = Path(dir / f'{self.directory.name}.csv')

        # Wrapping the actual saving into a timer as this might take some time.
        print(f"Saving merged DataFrame...")
        start_time = time.time()
        data.to_csv(dir, index=False)
        end_time = time.time()
        tqdm.write(f'File saved in {dir}. It took {end_time - start_time} seconds.')

    def process_data(self, data):
        """Processing data is simply creating a list of all DataFrames in memory"""
        self.data_list.append(data)

    def update_metadata(self):
        raise NotImplementedError("Not implemented for this subclass")


class PriceProcessor(FileProcessor):
    """File processor class to implement custom processing methods.
       Class is designed to take a specified method as an argument and save it internally before applying it to a DataFrame or all DataFrames inside a directory.
       Can also make use of predefined methods in method dictionary.
       Note: Method dictionary is currently empty and methods cannot be chained. Must pass own method.

       Intended workflow:
       - Load a directory into the class
       - pick a random sample using get_sample()
       - pass a method that takes a pd.DataFrame and any *args and **kwargs as argument and returns a pd.DataFrame using set_method
       - test the method using process_data() on PriceProcessor.sample
       - call process_directory() when the method applies the desired transformation
        """

    def __init__(self, directory, target_directory, method=None, method_kwargs={}, *args, **kwargs):
        super().__init__(directory, target_directory, *args, **kwargs)
        self.predefined_methods = process_prices.get_methods()
        self.set_method(method, **method_kwargs)


    def set_method(self, method, *args, **kwargs):
        """Store a custom function (callable) in the class instance for application to one file or to use for processing a directory

        Args:
            method (callable): Callable that at least takes a pd.DataFrame as argument and returns a pd.DataFrame. Will also take *args and **kwargs

        Raises:
            ValueError: trying to call a predefined method in the method dictionary that is not implemented.
            ValueError: object is not callable
        """

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

        self.method_args = args
        self.method_kwargs = kwargs


    def process_data(self, data: pd.DataFrame, method=None, *args, **kwargs):
        """Processes data using the defined custom function"""

        if method:
            self.set_method(method, *args, **kwargs)
        if self.method:
            self.last_processed = self.method(data, *self.method_args, **self.method_kwargs)
        else:
            raise ValueError("The method process_data requires a method to be set, but None was given.")
        
        # returning DataFrame so the method can also be called to directly transform a DataFrame.
        return self.last_processed

    def update_metadata(self):
        raise NotImplementedError("Subclasses must implement this method")

    


    
class StationProcessor(FileProcessor):
    """NYI"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_data(self, file):
        return process_stations.process_data(file)

    def update_metadata(self, data):
        return process_stations.update_metadata(data)


if __name__ == '__main__':
    """When __main__ is called, the 'Düsseldorf' subset will be applied to all data, reducing the number of stations to 130 down from 15,000.
       - Loads directories from config.paths
       - Processes all raw data using the RawPriceProcessor. Specifics of the transformation are defined in process_prices.
       - Any errors while processing directories will be caught and printed.
       - Saves metadata collected from all files. Metadata is currently average daily prices.
       
    """
    
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
