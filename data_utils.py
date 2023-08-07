# Copyright 2022 Carnegie Mellon University Neuromechatronics Lab
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

import re
import os
import glob
import shutil
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

    def __init__(self, 
                 subject=None, 
                 search_path=None, 
                 save_path=None, 
                 param_path=None, 
                 file_type='.db3', 
                 notebook_path = r'G:\Shared drives\NML_NHP\Monkey Training Records',
                 verbose=False, 
                 **kwargs):
        self.file_type = file_type
        self.subject = subject
        self.search_path = search_path       
        self.save_path = save_path
        self.param_path = param_path
        self.notebook_path = notebook_path
        self.verbose = verbose

        self.file_list = []
        self.date_list = []
        self.date = []
        self._data = None
        self.notebook_headers = None
        self.parser = BagParser()
        
        
    def check_path(self, data_path=None):
        """Checks the folder directory as a valid path and looks for bag files in subdirectories
        """
        if data_path is None:
            data_path = input("Please enter the parent folder directory: ")

        # check that the parent directory is the one desired
        if not os.path.isdir(data_path):
            print("Error - path not valid: {}".format(data_path))
            return False
        else:
            # Saves directory to object (might be useful)
            self.search_path = data_path
            if self.verbose:
                print('Valid root directory')
            return True
    
    
    def build_path(self, date, year_first=True):
        """ Helper function that creates folder subdirectories based on the date passed into the 
            proper subject and date-specified folders.
        
            Parameters
            ============
            date       : (str) String containing a date entry in the format of MM/DD/YY
            year_first : (bool) Boolean that switches the position of the year in the date entry 
                                to appear at the beginning instead of the end
                                
            Return
            ============
            save_folder_path  : (str) The absolute have to the saved data directory including the 
                             subject and modified date subdirectories
            new_date          : (str) The passed date with the new string format 
            
            
            Example:
            ============
            >> agent = DataAgent('/home/user/documents')
            >> save_path, new_date = agent.build_path('7/27/23', 'TestUser')
            >> print(save_path)
            >> '/home/user/documents/TestUser/TestUser_2023_07_27'
             
        """
        date_parts = date.split('/')
        if year_first:
            new_date = '20' + date_parts[2] + '_' + date_parts[0].zfill(2) + '_' + date_parts[1].zfill(2)
            #new_date =  '_'.join(['20'+date_parts[2]] + date_parts[:2])
        else:
            new_date = date_parts[0].zfill(2) + '_' + date_parts[1].zfill(2)  + '_' +'20' + date_parts[2] 
            #new_date = '_'.join(date_parts[:2]+['20'+date_parts[2]])
        save_folder_path = self.save_path + '\\' + self.subject + '\\' + self.subject + '_' + new_date + '\\'
        return save_folder_path, new_date
        
    def add_param_path(self, param_path=None):
        # Adds a search directory for parameters saved in config files
        if  os.path.isdir(param_path):
            self.param_path = param_path
        
        
    def get_cursor_pos(self, date, file_type='txt', save=False):
        # Saves the cursor position file in the directory specified as a .txt file by default unless also specified                   
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_CURSOR_POS.' + file_type
        
        print('Grabbing cursor position data...\n')
        df_file = self.get_topic_data('/cursor/position') # For just cursor position

        if self.verbose:
            print(df_file)

        if save:
            print('Saving cursor position data to:\n ' + (folder_path + file_name))            
            save_as(df_file, folder_path, file_name)
            print("Done")


    def get_states(self, date, file_type='txt', save=False):
    
        [folder_path, new_date] = self.build_path(date)            
        file_name = new_date + '_STATES.' + file_type
        print('Grabbing states...\n')
        df_file = self.get_topic_data('/task/center_out/state') # For just task states
        
        if self.verbose:
            print(df_file)
            
        if save:
            print('Saving task state events to:\n ' + (folder_path + file_name))        
            save_as(df_file, folder_path, file_name)
            print("Done")


    def get_targets(self, date, file_type='txt', save=False):
        """ Function that loads the target position data from a loaded bag file and stores it to a 
            local file (.txt by default)
        
            Parameters
            ----------
            date       : (str) String containing a date entry in the format of MM/DD/YY
            file_type  : (str) Date type of the log file
            save       : (bool) Option to save the data after loading
        """
    
        [folder_path, new_date] = self.build_path(date)            
        file_name = new_date + '_TARGETS.' + file_type
        print('Grabbing targets...\n')
        df_file = self.get_topic_data('/target/position') # For just task states

        if self.verbose:
            print(df_file)
            
        if save:
            print('Saving target position data to:\n ' + (folder_path + file_name))    
            save_as(df_file, folder_path, file_name)
            print("Done")


    def get_forces(self, date, file_type='txt', save=False):
        """ Function that loads the force transformation data for the robot and the cursor from a 
            loaded bag file and stores it to a local file (.txt by default)
        
            Parameters
            ----------
            date       : (str) String containing a date entry in the format of MM/DD/YY
            file_type  : (str) Date type of the log file
            save       : (bool) Option to save the data after loading

            
        """
        [folder_path, new_date] = self.build_path(date)
        file_name_1 = new_date + '_FORCE_ROBOT_FEEDBACK.' + file_type
        file_name_2 = new_date + '_FORCE_ROBOT_COMMAND.' + file_type
        file_name_3 = new_date + '_FORCE_CURSOR.' + file_type
        print('Grabbing force data...\n')
        f_robot_df_file = self.get_topic_data('/robot/feedback/force') # Force feedback data from the robot
        f_cmd_df_file = self.get_topic_data('/robot/command/force') # Force commands sent to the robot
        f_cursor_df_file = self.get_topic_data('/cursor/force') # Cursor force feedback

        if self.verbose:
            print(f_robot_df_file)
            
        if save:
            print('Saving force data to:\n ' + (folder_path + new_date + '_FORCE_* custom topic extension:\n  "ROBOT_FEEDBACK"\n  "ROBOT_COMMAND"\n  "CURSOR\n")' ))    
            save_as(f_robot_df_file, folder_path, file_name_1)
            save_as(f_cmd_df_file, folder_path, file_name_2)
            save_as(f_cursor_df_file, folder_path, file_name_3)
            print("Done")


    def get_metrics(self, date, file_type='txt', verbose=False, save=False):
        """ Saves the performance metrics of the loaded database in the directory specified as 
            a .txt file by default unless also specified
        
            Parameters
            ----------
            date       : (str) String containing a date entry in the format of MM/DD/YY
            file_type  : (str) Date type of the log file
            verbose    : (bool) Verbose text option
            save       : (bool) Option to save the data after loading 
        
        """
        
        
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_PERFORMANCE_METRICS.' + file_type

        print('Grabbing performance metrics...\n')        
        [N_trials, N_success, N_failure] = self.get_trial_performance()
        if self.param_path:
            msg_target_n = 'N_TARGETS:' + self.get_param_from_file(self.param_path, 'n_targets') + "\n"
            msg_target_r = 'TARGET_RADIUS:' + self.get_param_from_file(self.param_path, ['target_0','radius']) + "\n"
            msg_cursor_r = 'CURSOR_RADIUS:' + self.get_param_from_file(self.param_path, ['cursor','radius']) + "\n"
            msg_enforce_orient = 'ENFORCE_ORIENTATION:' + self.get_param_from_file(self.param_path, 'enforce_orientation') + "\n"
            msg_trial_t_avg = 'MEAN_TRIAL_T:' + str(self.get_mean_trial_time()) + "\n"

        if verbose:
            print("[{}] Performance metrics:\n\n".format(new_date))
            print("Total number of trials: {}".format(N_trials))
            print("Correct trials:         {}".format(N_success))
            print("Percentage correct:     {:.1f}".format(100*N_success/N_trials))

        if save:
            print('Saving metrics data to ' + (folder_path + file_name))
            with open((folder_path + file_name), "a") as f:
                # ===== Add any metadata information you want to save here ========
                msg = new_date + ",N:" + str(N_trials) + ",Success:" + str(N_success) + ",Failure:" + str(N_failure) + ",Success_Rate:" + str(100*N_success/N_trials) + "\n"
                f.write(msg)
                f.write(msg_trial_t_avg)

                if self.param_path:
                    f.write(msg_target_n)
                    f.write(msg_target_r)
                    f.write(msg_cursor_r)
                    f.write(msg_enforce_orient)
                # =================================================================
                f.close()

            print("Done")


    def save_all_data(self, date, file_type='txt'):
        """ Function that saves all the data collected from a log file into a data frame.
        """
        
        try:
            print('\n=== Grabbing all data ===\n')
            [folder_path, new_date] = self.build_path(date)          
            file_name = new_date + '_DATA.' + file_type

            for reader in self._data:
                print(reader)
                print(reader.records)
                df_file = pd.DataFrame(reader.records)
                if self.verbose:
                    print('Saving data to:\n  ' + (folder_path + file_name))
            
                save_as(df_file, folder_path, file_name)
        except:
            print("Something went wrong with saving the data... Please investigate")
            
            

    def move_files_to_data_dir(self, date, verbose=False):
        """ Function that moves the files found in the 'awaiting_process' subfolder contained in 
            the subject's raw data directory to the generated data directory. 
        
            Parameters
            ============
            date       : (str) String containing a date entry in the format of MM/DD/YY
            verbose    : (bool) Verbose text option
                                
            
            Example:
            ============
            >> import glob
            >>
            >> with open('/home/user/documents/file.txt', 'w') as fp:
            >> ...  pass
            >>
            >> root_path = '/home/user/documents'
            >> save_path = '/home/user/downloads'
            >> agent = DataAgent('TestUser', root_path, save_path)
            >>
            >> glob.glob(root_path)
            >> ['/home/user/documents/file.txt']
            >>
            >> agent.moves_files_to_data_dir('7/27/23')
            >> glob.glob(root_path)
            >> ['/home/user/documents/']
            >> glob.glob(save_path)
            >> ['/home/user/downloads/TestUser/TestUser_2023_07_27'/file.txt']
                    
        """
        
        # Creates a subfolder in the subjects data folder
        [folder_path, new_date] = self.build_path(date) 
        folder_path = os.path.join(self.search_path, '../', self.subject + '_' + new_date)
        if os.path.isdir(self.search_path):        
            dir_list = glob.glob(self.search_path + "/*")
            for dir_ in dir_list:
                if verbose: 
                    print("moving {} to {}".format(dir_, folder_path))
                shutil.move(dir_, folder_path)
                
                
                
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
        """
        
        
        
        """
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


    def get_param_from_file(self, param_path=None, param_name=None):
        """ Checks directory for config files in .yaml format, searches for the specified 
            parameter and returns an associated value
        
            Parameters
            ----------
            param_path : (str) directory for config files
            param_name : (str/list) Can be a single or list of strings indicating the parameter
                                    name (and subfields) to search for
            Return
            ---------
            param_val : (str) value associated with the parameter

        """
        
        if param_name is None:
            print("Please pass parameter name to search for")
            return
        
        if param_path is not None and type(param_path)==str:
            if os.path.isdir(param_path):
                self.param_dir = param_path
            else:
                return

        if not type(param_name) == list:
            param_name = [param_name] # Enforce being in a list for building regex search expression
        
        param_val = None
        file_list = glob.glob(self.param_dir + "*/*")
        if self.verbose: 
            print("Found files: \n")
            [print("{}".format(ff)) for ff in file_list]
            
        for file in file_list:
            with open(file, encoding = 'utf-8') as f:
                text_output = f.read()
                temp = [p + r':\s+' for p in param_name]
                str_exp = "(" + ''.join(temp) + ")([\S]*)"
                match = re.findall(str_exp, text_output)
                if match:
                    if self.verbose: print("Found parameter sequence '{}' in file '{}'. Getting value...".format(param_name, file))
                    param_val = match[0][1]
                    return param_val
                else:
                     if self.verbose: print("Could not find parameter '{}' in file '{}'".format(param_name, file))
                    
        if param_val is None:
            print("Could not find parameter '{}' in directory {}'".format(param_name, param_path))
  
        
    def get_files(self, file_path=None, file_type=None):
        """ Function that recursively goes through each folder from a received directory and returns a python dictionary with the file names for each 
            unique date detected. Days with multiple files in them are numerically arranged
        
            Parameters
            ----------
            file_path : (str) string containing an absolute directory to the data 
            file_type : (str) data file type
            
            Return
            ---------
            parsed_files : (dict) Python dictionary with dates for keys and lists of associated filenames for each unique date 
        """
        
        if file_path is not None and type(file_path)==str:
            if self.check_path(file_path):
                self.search_path = file_path
            else:
                return
        if file_type is not None and type(file_type)==str:
            self.file_type = file_type

        print("Searching for files...")
        self.file_list = glob.glob(self.search_path + "/**/*" + self.file_type, recursive=True)
        print("Found {} files with file type '{}'".format(len(self.file_list), self.file_type))
        if self.verbose:
            print("Files found in '{}' matching '{}' file type:".format(self.search_path, self.file_type))
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
        parsed_files = self.parse_files(data)

        return parsed_files
        
        
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
