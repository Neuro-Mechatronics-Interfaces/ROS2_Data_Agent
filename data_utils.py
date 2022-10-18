# Copyright 2022 Carnegie Mellon University Neuromechatronics Lab
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

import os
import glob
import re
from openpyxl import load_workbook, Workbook
import pandas as pd
import numpy as np
from nml_bag.reader import Reader

def save_as(df_file, folder_path, file_name):
    # Helper function to save files in a new directory
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        df_file.to_csv((folder_path + file_name))
        
        
def convert_to_utc(t_val):
    # Helper function to convert ROS2 nanosecond time format to python readable utc time
    # ex: '1661975258802625400' -> 1661975258.802625
    if not isinstance(t_val, str):
        t_val = str(t_val)
    
    return float(t_val[:10] + '.' + t_val[10:16])

    

class BagParser:
    """ A ROS2 bag file parser that utilizes the nml_bag 'Reader' Class and its ROS2 Bag file wrappers
       to collect information about the task experiment recorded.

    Parameters
    ----------
    *args : list
        Arguments passed to superclass constructor_.

        .. _constructor: https://github.com/ros2/rosbag2/blob
                         /fada54cc2c93d4238f3971e9ce6ea0006ac85c8c/rosbag2_cpp
                         /src/rosbag2_cpp/readers/sequential_reader.cpp#L60
    filepath : str
        Bag file path to open. If not specified, then the :py:func`open`
        function must be invoked before accessing any other class functionality.
    *kargs : dict
        Keyword arguments passed to superclass constructor_.

    === Below is the instruction summary of the rosbag parser object ===
    A ROS2 bag sequential reader class tailored to NML experimental data.

    Subclass of SequentialReader_ from the ros2bag_ package.

    .. _SequentialReader: https://github.com/ros2/rosbag2/blob/master
                          /rosbag2_cpp/src/rosbag2_cpp/readers
                          /sequential_reader.cpp
    .. _ros2bag: https://github.com/ros2/rosbag2#rosbag2

    -------------------------------------
    Notes from the 'Reader' class
    -------------------------------------

    In the current version of the ROS2 bag package, ``seek`` functionality has
    not yet been implemented_. Until this is implemented, we recommend opening
    a new :py:class`Reader` each time the position is to be reset.

    .. _implemented: https://github.com/ros2/rosbag2/blob
                     /fada54cc2c93d4238f3971e9ce6ea0006ac85c8c/rosbag2_cpp
                     /src/rosbag2_cpp/readers/sequential_reader.cpp#L198


    reader = Reader(f'{bag_path}{sep}bag_test_0.db3')
    reader.topics
    ['test_topic']
    reader.type_map
    {'test_topic': 'example_interfaces/msg/String'}
    reader.records
    [{'topic': 'test_topic', 'time_ns': ..., 'data': 'Hello World!'},
     {'topic': 'test_topic', 'time_ns': ..., 'data': 'Goodybye World!'}]
    tempdir.cleanup()

    """

    def __init__(self, files=None, verbose=False, **kwargs):
        self.verbose = verbose
        self.files = files
        self.reader_obj = None
        if files:
            self.reader_obj = self.read_bag_file(files)
            if self.verbose: print("Reading files completed")

    def read_bag_file(self, file_path):
        """ Opens the bag file specified by the parameter input.

        Parameters
        ----------
        file_path - Can be single string or list of strings to concatenate data from multiple files

        Returns
        -------
        obj - list of Reader object(s) containing ROS2 bag file data from each file read
        """
        print("Reading bag file")
        obj = []
        if type(file_path) == str:
            if self.verbose: print('Reading bag file from {}'.format(file_path))
            obj[0] = Reader(file_path)
        if type(file_path) == list:
            for file in file_path:
                if self.verbose: print('Reading bag file from {}'.format(file))
                obj.append(Reader(file))
        else:
            print('Error -  Be sure to pass a string directory or list of string directories')

        return obj

    def get_bag_topics(self, return_topics=False):
        """ Gets the topics found in the bag files. Defaults to the first bag object in memory, assumes other
         bag files have the same topics. Optio to return list of topics
         """
        print("Found topics: \n  " + self.reader_obj[0].topics)
        if return_topics:
            return self.reader_obj[0].topics

    def get_cursor_data(self):
        """ Extract rows matching topic '/cursor/position'.
            Output will be list of dict elements {position, timestamp, datatype, data}
        """
        df = self.get_bag_data('/cursor/position')
        return df




class DataAgent:
    """ A data sorting agent that searches a directory for matching file types and extracts file data with keyword inputs.

    Parameters
    ----------
    *args : (list) system arguments passed through to the constructor
    file_type: (str) file type to search for in all folders from root directory
    verbose: (bool) enable/disable verbose output
    """

    def __init__(self, file_type='.db3', verbose=False, root_dir=None, subject=None, **kwargs):
        self.parent_dir = None
        self.file_type = file_type
        self.notebook_headers = None
        self.verbose = verbose
        self.file_list = []
        self.date_list = []
        self._data = None
        self.parser = BagParser()
        self.root_dir = root_dir
        self.subject = subject
        self.date = []

        # These shouldn't need to change
        self.notebook_path = r'G:\Shared drives\NML_NHP\Monkey Training Records'

    def check_path(self, parent_folder=None):
        """Checks the folder directory as a valid path and looks for bag files in subdirectories
        """
        if parent_folder is None:
            parent_folder = input("Please enter the parent folder directory: ")

        # check that the parent directory is the one desired
        if not os.path.isdir(parent_folder):
            print("Error - path not valid: {}".format(parent_folder))
            return False
        else:
            # Saves directory to object (might be useful)
            self.parent_dir = parent_folder
            if self.verbose:
                print('Valid root directory')
            return True
    
    def build_path(self, date):
        date_parts = date.split('/')
        new_date = '_'.join(date_parts[:2]+['20'+date_parts[2]])
        folder_path = self.root_dir + '\\' + self.subject + '_' + new_date + '\\'
        return folder_path, new_date
        
    def save_cursor_pos(self, date, file_type='txt'):
        # Saves the cursor position file in the directory specified as a .txt file by default unless also specified
                    
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_CURSOR_POS.' + file_type
        
        print('Saving cursor position data to ' + (folder_path + file_name))
        df_file = self.get_topic_data('/cursor/position') # For just cursor position
        save_as(df_file, folder_path, file_name)
            
    def save_metrics(self, date, file_type='txt', verbose=False):
        # Saves the performance metrics of the loaded database in the directory specified as a .txt file by default unless also specified
        
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_PERFORMANCE_METRICS.' + file_type
        
        [N_trials, N_success, N_failure] = self.get_trial_performance()
        
        if verbose:
            print("Total number of trials: {}".format(N_trials))
            print("Successful trials:      {}".format(N_Success)
            print("Percentage correct:     {:.2f}".format(str(100*N_success/N_trials)))
            
        avg_trial_dur = self.get_mean_trial_time()
        
        print('Saving metrics data to ' + (folder_path + file_name))
        with open((folder_path + file_name), "a") as f:
            msg = new_date + ",N:" + str(N_trials) + ",Success:" + str(N_success) + ",Failure:" + str(N_failure) + ",Success_Rate:" + str(100*N_success/N_trials) + "\n"
            f.write(msg)
            f.write('MEAN_TRIAL_T:' + str(avg_trial_dur))
            f.close()
            
    def save_states(self, date, file_type='txt'):
    
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_STATES.' + file_type
            
        print('Saving task state events to ' + (folder_path + file_name))
        df_file = self.get_topic_data('/task/center_out/state') # For just task states
        save_as(df_file, folder_path, file_name)

    def set_notebook_headers(self, args, output=False):
        self.notebook_headers = args.split(',')
        if self.verbose:
            print('Filling in Notebook options:')
            if self.notebook_headers is not None:
                for n in self.notebook_headers:
                    print("\t" + n)
        else:
            print("No notebook data columns specified...")
        if output:
            return self.notebook_headers

    def get_notebook_data(self, animal, use_dataframe=True):
        """ Access the notebook training data from the NML_NHP shared drive for the animal specified

            TO-DO: auto-cycle through years
        """
        file_name = os.path.join(self.notebook_path, animal + '.xlsx')
        d = None
        w = None
        # check that the notebook directory is valid
        if not os.path.isfile(file_name):
            print("Error - path not valid: {}".format(file_name))
        else:
            print("Found notebook '{}.xlsx'".format(animal))
            if use_dataframe:
                dfs = pd.read_excel(file_name, None)
                d = dfs[list(dfs)[1]]  # Return notebook data for 2022
            else:
                d = load_workbook(filename=file_name, keep_vba=True)

        return d

    def update_notebook(self, nb_df, date_str, data):
        """ Updates the dataframe with the new data specified by the date and header name
        """
        for header in data:
            # index of date
            date_idx = nb_df.index[nb_df[nb_df.columns[0]] == date_str].tolist()[0]
            nb_df.loc[date_idx, header] = data[header]  # replace date,header index with new value

        return nb_df

    def check_notebook_entry(self, table, date_str, headers, overwrite=False):
        """ Check which headers are empty for new date entry, if not then skip
        """
        if headers is not None:
            skip = [False for i in range(len(headers))]
            for h, header in enumerate(headers):
                if isinstance(table, pd.DataFrame):
                    if header not in table.columns:
                        if self.verbose: print("Warning - {} not found in data frame columns".format(header))
                    else:
                        date_idx = table.index[table[table.columns[0]] == date_str].tolist()[0]
                        if not np.isnan(table.loc[date_idx, header]):
                            if self.verbose: print("Warning - {} already contains data. Skipping")
                            if not overwrite:
                                skip[h] = True
                if isinstance(table, Workbook):
                    # Only supporting dataframes for now
                    pass
        else:
            print("No header field names specified")
            skip = None

        return skip

    def parse_str_date_info(self, string, year_format=None):
        """ Internal parser function to extract the date from a file name

        date_str: (str) output string in a date format of 'm/d/yy' (can be changed if notebook changes)
        """
        temp = re.search('[0-9]{6}', string)
        dd = int(temp[0][4:6])
        mm = int(temp[0][2:4])
        yy = int(temp[0][:2])
        info_str = str(mm) + "/" + str(dd) + "/" + str(yy)

        return info_str, temp.group()

    def parse_files(self, data):
        """"""
        parsed_data = {}
        for date_str, str_list in data.items():
            if date_str not in parsed_data:
                parsed_data[date_str] = None

            # Get times and file keys
            times_list = []
            keys_list = []
            for string in str_list:
                temp = re.findall('(\d+)(?=.)', string)
                times_list.append(temp[3])
                keys_list.append(temp[4])

            max_idx = [i for i, value in enumerate(keys_list) if value == max(keys_list)]
            time_match = times_list[max_idx[-1]]  # time substring files
            if self.verbose:
                print("Largest key found: {} from {}".format(keys_list[max_idx[-1]], time_match))

            # Fill new dictionary with filtered list of files
            parsed_data[date_str] = [j for j in str_list if time_match in j]

        print("Sorted files with most consecutive keys")
        return parsed_data
        
    def has_data(self):
        if self._data is not None:
            return True
        else:
            return False
            
    def read_files(self, files=None):
        self._data = self.parser.read_bag_file(files)
        
    def get_files(self, file_path=None, file_type=None):
        """ Recursively goes through each folder and returns a dict with the file names for each day.
            Days with multiple files in them are numerically arranged
        """
        
        if file_path is not None and type(file_path)==str:
            self.parent_dir = file_path
        if file_type is not None and type(file_type)==str:
            self.file_type = file_type

        print("Searching for files...")
        self.file_list = glob.glob(self.parent_dir + "/**/*" + self.file_type, recursive=True)
        print("Found {} files with file type '{}'".format(len(self.file_list), self.file_type))
        if self.verbose:
            print("Files found in '{}' matching '{}' file type:".format(self.parent_dir, self.file_type))
            for item in self.file_list:
                print("\t" + item)

        # Collect all dates present in files to initialize dictionary
        data = {}
        for item in self.file_list:
            date_str, matched_str = self.parse_str_date_info(item)
            if date_str not in data:
                self.date_list.append([date_str, matched_str])
                data[date_str] = None

        # Fill dictionary date keys with lists of corresponding files
        for date in self.date_list:
            data[date[0]] = [i for i in self.file_list if date[1] in i]
            if self.verbose:
                print("Files matching {}: {}".format(date[1], data[date[0]]))

        # Before returning list, sort files by arranging them in specified order
        filtered_data = self.parse_files(data)

        return filtered_data
        
    def get_topic_data(self, topic, options=None):
        """
            Parameters
            ----------
            bag_topic : (str) topic to access
            options : (list) list containing keywords for data types ('cursor' | 'state') TO-DO

            Return
            ---------
            parsed_topic_df : (Dataframe) pandas dataframe table of parsed topic data

            Notes
            --------
            The 'options' parameter can be expanded for more unique data types as DataAgent evolves

        """
        parsed_topic_data = []
        if not type(topic) == str:
            print("bag topic input must be string")
            return None

        if self.verbose:
            print("Grabbing data from {}".format(topic))
        
        reader_obj = self._data
        for reader in reader_obj:
            for el in reader.records:
                if el['topic'] == topic:
                    parsed_topic_data.append(el)

        if len(parsed_topic_data) > 0:
            parsed_topic_df = pd.DataFrame(parsed_topic_data)
        else:
            print("Warning - Could not find data with topic {}".format(topic))
            parsed_topic_df = None
        return parsed_topic_df

    def get_trial_performance(self):
        """ Helper function that gets the task performance statistics

        Returns
        ---------
        N_trials: (int) total trials from success and failure combined states
        N_success: (int) total success states
        N_failure: (int) total failure states
        """
        df = self.get_topic_data('/task/center_out/state')
        if df is not None:
            n_success = len(df[df['data'] == 'success'])
            n_failure = len(df[df['data'] == 'failure'])
            n_trials = n_success + n_failure
            return n_trials, n_success, n_failure
        else:
            return 0, 0, 0

    def get_mean_trial_time(self, start_state='move_a'):
        """ Function that finds the average duration of trials from the task state changes
        
        """
        # Not sure if this will be the more accurate way to do it, but here's one version
        # In loop for length of data
        # Grab indices of success states from last to first
        # find index of first occurence of hold_a state. 
        # Get time difference between states
        # Find next success state continue
        # subtract SUCCESS state time with MOVE_A state time
        
        # For now we find the time between all hold_a states
        df_states = self.get_topic_data('/task/center_out/state') # For just task states
        
        start_idx = df_states.index[df_states['data'] == start_state].tolist()
        start_t = [convert_to_utc(i) for i in df_states['time_ns'][start_idx]]
        mean_total_trial_t = np.mean(np.diff(start_t))
        return mean_total_trial_t