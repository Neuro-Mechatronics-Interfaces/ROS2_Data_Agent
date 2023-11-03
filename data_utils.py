# Copyright 2022 Carnegie Mellon University Neuromechatronics Lab
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

import re
import os
import glob
import time
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

class ArgParser:
    """ An argument parser that searches for keywords in a scanned text and outputs the value associated 
        with the matched key with a filled-in dictionary
    
    Parameters
    ==========
    keys:  (list)
    text_path: (str) path to the directory containing the 'config.txt' file
    delimiter: (str) Delimiter character to split text lines with
    verbose: (bool) enable/disable verbose output
    skip_empty: (bool) enable/diable the option to include keys with empty values
    
    Returns
    =======
    parsed_args: (dict) A dictionary containg keys with each of the parameters discovered in a text file.
    
    """

    def __init__(self, file_path=None, delimiter="=", args=None, verbose=False, skip_line_flag=("#", " ", "\n"), skip_empty=True):
        self.file_path = file_path
        self.delimiter = delimiter
        self.args = args
        self.verbose = verbose
        self.skip_flag = skip_line_flag # Has to be a single item or tuple
        self.skip_empty = skip_empty
        self.lowercase = True
        #self.scan_file(text_path, delimiter)
    
    def scan_file(self, file_path=None, delimiter=None):
        """ Scans a file for key-value pairs of parameters. Checks the current directory for the file if no directory is specified
        
        Parameters
        ==========
        file_path: (str) (optional) Absolute file path for the file to read, optional parameter if already specified in object
        delimiter: (str) (optional) Delimiter to parse values from keys, optional parameter if already specified in object
        
        Results
        ==========
        parsed_args: (dict) Python dictionary type with the key and value pairs
        """
        
        if delimiter:
            delim = delimiter
        else:
            delim = self.delimiter
        
        if self.file_path:
            text_path = self.file_path
        elif file_path:
            text_path = file_path
        else:
            text_path = os.path.join(os.getcwd(),'config.txt')
            
        with open(text_path) as f:
            lines = f.readlines()
            if self.verbose: print(lines)
            
            parsed_args = {}
            for line in lines:
                
                temp = line.split(delim, 1) # first occurence
                if len(temp)!=2: 
                    if self.verbose: print('Parameter missing delimiter on line')
                elif temp[1]=='\n' and self.skip_empty:
                    if self.verbose: print('Empty parameter detected, skipping\n')
                elif temp[0].startswith(self.skip_flag):
                    pass # skip line
                else:
                    val = re.sub('\n','',temp[1]) # Grab new parameters by checking for new lines
                    if self.verbose: print(val)
                    if self.lowercase:
                        temp[0] = temp[0].lower()
                    parsed_args[temp[0]] = val
                    
            # Override parameters if found in args
            if self.args:
                for i, arg in enumerate(self.args):
                    parsed_args[i] = arg
                    
            return parsed_args


class DataAgent:
    """ A data sorting agent that searches a directory for matching file types and extracts file data with keyword inputs.

    Parameters
    ----------
    *args :      (list) system arguments passed through to the constructor
    subject:     (str) Name of the subject to identify data with
    search_path: (str) Directory for raw data files
    save_path:   (str) Directory for the generated data files
    param_path:  (str) directory for the task's .yaml config files for metadata collection
    file_type:   (str) file type to search for in all folders from root directory
    verbose:     (bool) enable/disable verbose output
    *kargs :     (dict) Keyword arguments passed to superclass constructor_.
    """

    def __init__(self, 
                 subject=None, 
                 search_path=None, 
                 save_path=None, 
                 param_path=None, 
                 file_type=".db3", 
                 notebook_path = r'',
                 verbose=False, 
                 **kwargs):
        self.verbose = verbose
        self.file_type = file_type
        self.subject = subject
        self.search_path = search_path 
        self.save_path = save_path 
        self.param_path = param_path
        self.notebook_path = notebook_path
        self.file_list = []
        self.date_list = []
        self.date = []
        self._data = []
        self.notebook_headers = None
        
        # Check that the paths exist
        if self.search_path:
            self.check_path(self.search_path)
        if self.save_path:
            self.check_path(self.save_path)
        if self.param_path:
            self.check_path(self.param_path)
            

    def add_param_path(self, param_path=None):
        """ Adds a search directory for parameters saved in config files 
        """
        if  os.path.isdir(param_path):
            self.param_path = param_path
    
    
    def build_path(self, date, year_first=True):
        """ Helper function that creates folder subdirectories based on the date passed into the 
            proper subject and date-specified folders.
        
        Parameters:
        ----------
        date       : (str)  String containing a date entry in the format of MM/DD/YY
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
        if self.save_path:
            parent_folder = self.save_path
        else:
            parent_folder = ''
        save_folder_path = parent_folder + '\\' + self.subject + '\\' + self.subject + '_' + new_date + '\\'
        return save_folder_path, new_date

                
    def check_path(self, data_path=None):
        """Checks the folder directory as a valid path and looks for bag files in subdirectories
        
        Parameters:
        ----------
        data_path : (str) Absolute path directory to folder
        
        Return:
        ----------
        data_path : (str, None) Returns the same data path provided if valid path, otherwise None
        
        """
        if data_path is None:
            data_path = input("Please enter the parent folder directory: ")
            
        if os.path.isdir(data_path):
            if self.verbose: print('Valid root directory')
            return data_path
        else:
            print("Warning, path doesn't exist. PLease fix path and retry, otherwise path will be set to 'None':\n\n{}".format(data_path))
            return None
    
        
    def check_notebook_entry(self, table, date_str, headers, overwrite=False):
        """ Check which headers are empty for new date entry, if not then skip
        
        Parameters:
        ----------
        table     :
        date_str  :
        headers   :
        overwrite :
        
        Return:
        ----------
        skip      : (list) A list of boolean values indexed by the argument header
        
        """
        if headers is None:
            print("Warning, no header field names specified. Returning 'None'")
            return None
        else:
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

            return skip        
    
                
    def get_notebook_data(self, animal, use_dataframe=True):
        """ Access the notebook training data from the subjected specified

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


    def get_cursor_pos(self, date, topic='/environment/cursor/position', file_type='txt', save=False, overwrite=False):
        """ Helper function that loads the cursor position data from a loaded bag file and stores it 
            to a local file (.txt by default)
        
        Parameters
        ----------
        date       : (str) String containing a date entry in the format of MM/DD/YY
        topic      : (str) ROS2 topic containing the information
        file_type  : (str) Date type of the log file
        save       : (bool) Option to save the data after loading
        overwrite  : (bool) Option to overwrite existing file
        
        """
                 
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_CURSOR_POS.' + file_type
        
        if os.path.isfile(os.path.join(folder_path, file_name)):
           print("File '{}' in directory '{}' already exists. Overwriting not supported. Please delete the file and resave".format(file_name, folder_path))
           return

        print('Grabbing cursor position data...\n')
        df_file = self.get_topic_data(topic) # For just cursor position

        if save and df_file is not None:
            if os.path.exists((folder_path + file_name)):
                if not overwrite:
                    print('{} already in path. Set enable to True if you would like to save a new file'.format((folder_path + file_name)))
                else:
                    print('Saving cursor position data to:\n ' + (folder_path + file_name))            
                    save_as(df_file, folder_path, file_name)
                    
        print("Done")


    def get_states(self, date, topic='/machine/state', file_type='txt', save=False, overwrite=False):
        """ Helper function that loads the task states from a loaded bag file and stores it to a 
            local file (.txt by default)
        
        Parameters
        ----------
        date       : (str) String containing a date entry in the format of MM/DD/YY
        topic      : (str) ROS2 topic containing the information
        file_type  : (str) Date type of the log file
        save       : (bool) Option to save the data after loading
        overwrite  : (bool) Option to overwrite existing file
        
        """
    
        [folder_path, new_date] = self.build_path(date)            
        file_name = new_date + '_STATES.' + file_type
        #print(os.path.join(folder_path, file_name))
        if os.path.isfile(os.path.join(folder_path, file_name)):
           print("File '{}' in directory '{}' already exists. Overwriting not supported. Please delete the file and resave".format(file_name, folder_path))
           return
        
        print('Grabbing states...\n')
        df_file = self.get_topic_data(topic) # For just task states
            
        if save and df_file is not None:
            if os.path.exists((folder_path + file_name)):
                if not overwrite:
                    print('{} already in path. Set enable to True if you would like to save a new file'.format((folder_path + file_name)))
                else:
                    print('Saving data to:\n ' + (folder_path + file_name))            
                    save_as(df_file, folder_path, file_name)
                    
        print("Done")
                

    def get_emg(self, date, topic='/pico_ros/emg/ch1', file_type='txt', save=False, overwrite=False):
        """ Helper function that loads the electromyographic recordings of a specific channel from a 
            loaded bag file and stores it to a local file (.txt by default)
        
        Parameters:
        ----------
        date       : (str) String containing a date entry in the format of MM/DD/YY
        topic      : (str) ROS2 topic containing the information
        file_type  : (str) Date type of the log file
        save       : (bool) Option to save the data after loading
        overwrite  : (bool) Option to overwrite existing file
        
        """
        [folder_path, new_date] = self.build_path(date)            
        file_name = new_date + '_EMG.' + file_type
        
        if os.path.isfile(os.path.join(folder_path, file_name)):
           print("File '{}' in directory '{}' already exists. Overwriting not supported. Please delete the file and resave".format(file_name, folder_path))
           return
           
        print('Grabbing EMG data...\n')
        df_file = self.get_topic_data(topic) # For just task states
        
        if save and df_file is not None:
            print('Saving emg data to:\n ' + (folder_path + file_name))    
            save_as(df_file, folder_path, file_name)
        
        print("Done")


    def get_targets(self, date, topic='/environment/target/position', file_type='txt', save=False, overwrite=False):
        """ Helper function that loads the target position data from a loaded bag file and stores it 
            to a local file (.txt by default)
        
        Parameters:
        ----------
        date       : (str) String containing a date entry in the format of MM/DD/YY
        topic      : (str) ROS2 topic containing the information
        file_type  : (str) Date type of the log file
        save       : (bool) Option to save the data after loading
        overwrite  : (bool) Option to overwrite existing file
        """
    
        [folder_path, new_date] = self.build_path(date)            
        file_name = new_date + '_TARGETS.' + file_type
        if os.path.isfile(os.path.join(folder_path, file_name)):
           print("File '{}' in directory '{}' already exists. Overwriting not supported. Please delete the file and resave".format(file_name, folder_path))
           return
        
        print('Grabbing targets...\n')
        df_file = self.get_topic_data(topic) # For just task states
            
        if save and df_file is not None:
            print('Saving target position data to:\n ' + (folder_path + file_name))    
            save_as(df_file, folder_path, file_name)
            print("Done")


    def get_forces(self, date, file_type='txt', save=False, overwrite=False):
        """ Function that loads the force transformation data for the robot and the cursor from a 
            loaded bag file and stores it to a local file (.txt by default)
        
        Parameters:
        ----------
        date       : (str) String containing a date entry in the format of MM/DD/YY
        file_type  : (str) Date type of the log file
        save       : (bool) Option to save the data after loading
        overwrite  : (bool) Option to overwrite existing file
            
        """
        [folder_path, new_date] = self.build_path(date)
        file_name_1 = new_date + '_FORCE_ROBOT_FEEDBACK.' + file_type
        file_name_2 = new_date + '_FORCE_ROBOT_COMMAND.' + file_type
        file_name_3 = new_date + '_FORCE_CURSOR.' + file_type
        if os.path.isfile(os.path.join(folder_path, file_name_1)):
           print("File '{}' in directory '{}' already exists. Overwriting not supported. Please delete the file and resave".format(file_name, folder_path))
           return

        
        print('Grabbing force data...\n')
        f_robot_df_file = self.get_topic_data('/robot/feedback/force') # Force feedback data from the robot
        f_cmd_df_file = self.get_topic_data('/robot/command/force') # Force commands sent to the robot
        f_cursor_df_file = self.get_topic_data('/cursor/force') # Cursor force feedback
            
        if save:
            print('Saving force data to:\n ' + (folder_path + new_date + '_FORCE_* custom topic extension:\n  "ROBOT_FEEDBACK"\n  "ROBOT_COMMAND"\n  "CURSOR\n")' ))    
            save_as(f_robot_df_file, folder_path, file_name_1)
            save_as(f_cmd_df_file, folder_path, file_name_2)
            save_as(f_cursor_df_file, folder_path, file_name_3)
            print("Done")


    def get_metrics(self, date, topic='/machine/state', file_type='txt', verbose=False, save=False, overwrite=False):
        """ Saves the performance metrics of the loaded database in the directory specified as 
            a .txt file by default unless also specified
        
        Parameters
        ----------
        date       : (str) String containing a date entry in the format of MM/DD/YY
        file_type  : (str) Date type of the log file
        verbose    : (bool) Verbose text option
        save       : (bool) Option to save the data after loading 
        overwrite  : (bool) Option to overwrite existing file
        
        """
        
        [folder_path, new_date] = self.build_path(date)
        file_name = new_date + '_PERFORMANCE_METRICS.' + file_type

        if os.path.isfile(os.path.join(folder_path, file_name)) and not overwrite:
           print("File '{}' in directory '{}' already exists and overwriting set to False. Please set overwrite to True or delete the file".format(file_name, folder_path))
           return
           
        print(' | Grabbing performance metrics...')
        [N_trials, N_success, N_failure] = self.get_trial_performance(topic)
        avg_trial_t = self.get_mean_trial_time(topic)
        if N_trials is None:
            print(" | Failed to get performance metrics")
            return

        # Display the trial perfoemance metrics on screen
        if self.verbose or verbose:
            print("[{}] Performance metrics:\n\n".format(new_date))
            print("Total number of trials: {}".format(N_trials))
            print("Correct trials:         {}".format(N_success))
            print("Percentage correct:     {:.1f}".format(100*N_success/N_trials))
            
        # Decided to add paramters to metrics file, don't need to make new file to parse yet
        if not self.param_path:
            print('Warning - Parameter file path not set. USe the add_param_path() function with the directory holding your parameter files.')
        else:
            n_targets = self.get_param_from_file(self.param_path, 'n_targets') 
            target_r = self.get_param_from_file(self.param_path, ['target','radius'])
            cursor_r = self.get_param_from_file(self.param_path, ['cursor','radius'])
            #enforce_orient = self.get_param_from_file(self.param_path, 'enforce_orientation')

        if save:
            print('Saving metrics data to ' + (folder_path + file_name))
            with open((folder_path + file_name), "a") as f:
            
                # ===== Add any metadata information you want to save here ========
                msg = new_date + ",N:" + str(N_trials) + ",Success:" + str(N_success) + ",Failure:" + str(N_failure) + ",Success_Rate:" + str(100*N_success/N_trials) + "\n"
                f.write(msg)
                
                if avg_trial_t is not None:
                    f.write('MEAN_TRIAL_T:' + str(avg_trial_t) + "\n")
                
                if n_targets is not None:
                    f.write('N_TARGETS:' + n_targets + "\n")

                if target_r is not None:
                    f.write('TARGET_RADIUS:' + target_r + "\n")

                if cursor_r is not None:
                    f.write('CURSOR_RADIUS:' + cursor_r + "\n")

                #if enforce_orient is not None:
                #    f.write('ENFORCE_ORIENTATION:' + enforce_orient + "\n"
                
                # =================================================================
                f.close()

            print("Done")


    def get_param_from_file(self, param_path=None, param_name=None, file_type='yaml'):
        """ Checks directory for config files in .yaml format, searches for the specified 
            parameter and returns an associated value
        
        Parameters:
        ----------
        param_path : (str) directory for config files
        param_name : (str/list) Single or list of strings of parameter name (and subfields) to search
        file_type  : (str) The file format to look specifically for (default: yaml)
            
        Return:
        ---------
        param_val : (str) value associated with the parameter

        """
        
        if param_name is None:
            print("Please pass parameter name to search for and try again")
            return

        if type(param_name) is not list:
            param_name = [param_name] # Enforce being in a list for building regex search expression
        
        param_val = None
        file_list = glob.glob(self.param_path + "*/*." + file_type)
        if self.verbose: 
            print("Found files: \n")
            [print("{}".format(ff)) for ff in file_list]
            
        # Check each file for the parameter until found
        param_found = False
        while not param_found:
            for file in file_list:
                with open(file, encoding = 'utf-8') as f:
                    text_output = f.read()
                    temp = [p + r':\s+' for p in param_name]
                    str_exp = "(" + ''.join(temp) + ")([\S]*)"
                    match = re.findall(str_exp, text_output)
                if match:
                    if self.verbose: print("Found parameter sequence '{}' in file '{}'. Getting value...".format(param_name, file))
                    param_val = match[0][1]
                    param_found = True
                    return param_val
                else:
                    if self.verbose: print("Could not find parameter '{}' in file '{}'".format(param_name, file))
                    
        if param_val is None:
            print("Could not find parameter '{}' in directory {}'".format(param_name, param_path))


    def get_cursor_data(self):
        """ Extract rows matching topic '/cursor/position'.
            Output will be list of dict elements {position, timestamp, datatype, data}
        """
        df = self.get_bag_data('/cursor/position')
        return df
        
                
    def get_topic_data(self, topic, options=None):
        """ Function that returns a datafram object containing the topic's data if present
        
        Parameters:
        ----------
        bag_topic       : (str) topic to access
        options         : (list) list containing keywords for data types ('cursor' | 'state') TO-DO
   
        Return:
        -----------
        parsed_topic_df : (Dataframe) pandas dataframe table of parsed topic data

        Notes
        --------
        The 'options' parameter can be expanded for more unique data types as DataAgent evolves
        
        """
        
        if not type(topic) == str:
            print("Warning: topic input must be string")
            return None

        parsed_topic_data = []
        print("Searching for '{}' topic data...00%".format(topic), end="")
        #print("\b\b\b{:02d}%".format(val), end="")
        for i, reader in enumerate(self._data):
                    
            print("\b\b\b{:02d}%".format( int( 100*(i+1)/(len(self._data) + 1) ) ))
            for el in reader.records:
                if el['topic'] == topic:
                    parsed_topic_data.append(el)
            if len(parsed_topic_data) == 0:
                print("Warning, topic '{}' not found in bag file, Stopping search.".format(topic))
                return None

        print("\b\b\b\bDone\n=======================================================")
        parsed_topic_df = None
        if len(parsed_topic_data) > 0:
            parsed_topic_df = pd.DataFrame(parsed_topic_data)
            if self.verbose:
                print(parsed_topic_df)
            return parsed_topic_df
        else:
            print("Warning: Could not find the topic '{}' in the loaded files. Check topic spelling".format(topic))
            return None
        

    def get_trial_performance(self, topic='/machine/state'):
        """ Helper function that gets the task performance statistics. Reads the state topic to 
            evaluate performance (default: /a\machine/state)
        
        Parameters:
        -----------
        topic     : (str) The ROS2 topic to read task states

        Returns
        ---------
        N_trials  : (int) total trials from success and failure combined states
        N_success : (int) total success states
        N_failure : (int) total failure states
        
        """
        df = self.get_topic_data(topic)
        if df is not None:
            n_success = len(df[df['data'] == 'success'])
            n_failure = len(df[df['data'] == 'failure'])
            n_trials = n_success + n_failure
            return n_trials, n_success, n_failure
        else:
            return None, None, None


    def get_mean_trial_time(self, topic='task/state', start_state='move_a'):
        """ Function that finds the average duration of trials from the task state changes
        
        Parameters:
        -----------
        topic              : (str) The ROS2 topic to read task states
        start_state        : (str) Starting state name to reference trial times with

        Returns
        ---------
        mean_total_trial_t : (float)
        
        Note:
          Not sure if this will be the more accurate way to do it, but another method below:
           - In loop for length of data
           - Grab indices of success states from last to first
           - find index of first occurence of hold_a state. 
           - Get time difference between states
           - Find next success state continue
           - subtract SUCCESS state time with MOVE_A state time
          
        """
        
        # For now we find the time between all hold_a states
        df_states = self.get_topic_data(topic) # For just task states        
        if df_states is not None:
            start_idx = df_states.index[df_states['data'] == start_state].tolist()
            start_t = [convert_to_utc(i) for i in df_states['time_ns'][start_idx]]
            mean_total_trial_t = np.mean(np.diff(start_t))
        else:
            print("Error getting data from topic {}".format(topic))
            mean_total_trial_t = None
        
        return mean_total_trial_t


    def has_data(self):
        if len(self._data) > 0:
            return True
        else:
            return False
    
    
    def transfer_data(self, files=None, new_path=None, tag=None, move_parent_folder=False):
        """ Function that moves the files defined as filepath strings to a new directory.
         
        
        Parameters:
        -----------
        files              : (str/list) Filepath string or list of filepath strings to move, can be 
                               set to None to select files in the search directory
        new_path           : (str) replacement directory if not using the save path
        tag                : (str) String containing a date entry in the format of MM/DD/YY
        move_parent_folder : (bool) Option to move the folder containing the data
            
            
        Note:
        ------------
        Using shutil to transfer files or directories between disk drives has been known to thrown 
        errors. This can happen if write permissions aren't set for the new directory. Using 'rsync' 
        in a bash script would be a alternative method
        
        
        Example 1, move single file:
        ---------------------------
        >> search_path = '/home/user/documents'
        >> save_path = '/home/user/downloads'
        >> agent = DataAgent('TestUser', search_path, save_path)
        >>
        >> with open('/home/user/documents/myfile.txt', 'w') as fp: # create empty text file
        >> ...  pass
        >> agent.transfer_data('/home/user/documents/myfile.txt','/home/user/downloads/myfile.txt')
        >>
        >> import glob
        >> glob.glob(save_path)
           ['/home/user/downloads/TestUser/myfile.txt']
        
        
        Example 2, move and rename folder containing files in search path using tag
        ---------------------------
        >> date = '7/27/23' # Date tag entry in the format of MM/DD/YY
        >>
        >> import os
        >> os.mkdir('/home/user/documents/MyFolder')
        >> with open('/home/user/documents/MyFolder/myfile2.txt', 'w') as fp:
        >> ...  pass
        >> with open('/home/user/documents/MyFolder/myfile3.txt', 'w') as fp:
        >> ...  pass
        >>
        >> agent.transfer_data(None, save_path, date)
        >>
        >> glob.glob(search_path)
           ['/home/user/documents']
        >> glob.glob(save_path)
           ['/home/user/downloads/TestUser/TestUser_2023_07_27'/myfile2.txt', 
            '/home/user/downloads/TestUser/TestUser_2023_07_27'/myfile3.txt']
                    
        """        
        
        if not (new_path or tag):
            print("Error - Please specify either new directory or tag")
            return
        elif not new_path:
            print("Error - Please specify at least a new directory")
            return
        
        if not os.path.isdir(new_path):
            print("Error - Please enter valid new path")
            return
            
        if files:           
            file_list = (files if type(files) is list else [files])
        else:
            # Get all the files located in the search path if no file is provided
            file_list = glob.glob(self.search_path + "/*")
            if not len(file_list)>0:
                print('Error - No files or folders found in search directory')
                return
            
        # If true, grabbing the parent directory of the file
        if move_parent_folder:
            print("'move_parent_folder' set to True, moving all contents in parent folder")
            file_list = [str(os.path.dirname(file_list[0]))]            
            
        if self.verbose:
            print("Preparing data to transfer: ")
            [print(" | [{}]".format(i)) for i in file_list]
        
        folder_path = os.path.join(new_path, self.subject)
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

        if tag:
            [ _, new_date] = self.build_path(tag) 
            folder_path = os.path.join(folder_path, self.subject + '_' + new_date)
            
        for file_ in file_list:
            if os.path.isfile(file_):
                file_name = os.path.basename(file_)
                transfer_path = os.path.join(folder_path, file_name)
                if os.path.isfile(transfer_path):
                    print(" | | Stopping transfer of {} to {}, file already exists".format(file_, transfer_path))     
                    continue
            if os.path.isdir(file_):
                transfer_path = folder_path
                if os.path.isdir(transfer_path):
                    print(" | | Stopping transfer of {} to {}, folder already exists".format(file_, transfer_path))   
                    continue
                
            if self.verbose:
                print("Moving file:\n | Original path: {}\n | New path {}".format(file_, transfer_path))
            
            # Using the 'shutil.move()' method throws two error messages: [Errno 18, Errno 95] 
            try:
if os.path.isdir(transfer_path):
                print("About to delete directory in 60 seconds. Check that the folder has been successfully transfered")
                time.sleep(60)
                shutil.move(os.path.realpath(file_), transfer_path)
            except e:
                print(e)

        
    def parse_files(self, data):
        """ Rearranges the order of files stored in a dictionary associated with a specific training 
            day by specific ending index
            
        Parameters:
        -----------
        data        : (dict) Dictionary with dates for keys and file name paths for values
        
        Returns:
        -----------
        parsed_data : (dict) Dictionary with the same values associated with the keys but reararnged
        
        Example: 
        -----------
        TO-DO
                
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


    def parse_str_date_info(self, string, year_format=None):
        """ Internal parser function to extract the date from a file name

        Parameters:
        ----------
        date_str: (str) output string in a date format of 'm/d/yy' (can be changed if notebook 
                        changes)
                        
        Returns:
        ----------
        info_str: (str) Reformatted string in the generated_data folder date format
        temp    : (str) last match from regular expression 
                               
        """
        temp = re.search('[0-9]{6}', string)
        dd = int(temp[0][4:6])
        mm = int(temp[0][2:4])
        yy = int(temp[0][:2])
        info_str = str(mm) + "/" + str(dd) + "/" + str(yy)

        return info_str, temp.group()            


    def read_bag(self, file_path, display_topics=False):
        """ Opens and reads the bag file specified by the parameter input.

        Parameters:
        ----------
        file_path : (str) Single string or list of strings to concatenate data from multiple files

        Returns:
        ----------
        obj      : (list) list of Reader object(s) containing ROS2 bag file data from each file read
        
        """
        obj = []

        # Need to define the storage ID with the Reader object before attempting to access files 
        ft = self.file_type
        if ft == '.db3' or ft == 'db3' or ft == 'sqlite3':
            storage_id = 'sqlite3'
        elif ft == '.mcap' or ft == 'mcap':
            storage_id = 'mcap'

        if type(file_path) == str:
            print(' | Reading bag file from {}'.format(file_path))
            self._data[0] = Reader(file_path, storage_id=storage_id)
        elif type(file_path) == list:
            for f in file_path:
                print(' | Reading bag file from {}'.format(f))
                self._data.append(Reader(f, storage_id=storage_id))
        else:
            print('Error - Be sure to pass a string directory or list of string directories')
            return

        if display_topics:
            for obj in self._data:
                print("Topics present: ")
                print("{}\n".format([i for i in obj.topics]))
                
    

    def save_all_data(self, date, file_type='txt'):
        """ Function that saves all the data collected from a log file into a data frame.
        """
        
        try:
            print('\n=== Grabbing all data ===\n')
            [folder_path, new_date] = self.build_path(date)          
            file_name = new_date + '_DATA.' + file_type

            for reader in self._data:
                #if self.verbose: print(reader)
                #if self.verbose: print(reader.records)
                df_file = pd.DataFrame(reader.records)
                print('Saving data to:\n  ' + (folder_path + file_name))            
                save_as(df_file, folder_path, file_name)
        except:
            print("Something went wrong with saving the data... Please investigate")


    def search_for_files(self, file_path=None, file_type=None):
        """ Function that recursively goes through each folder from a received directory and returns a 
            python dictionary with the file names for each unique date detected. Days with multiple 
            files in them are numerically arranged.
        
        Parameters
        ----------
        file_path : (str) string containing an absolute directory to the data 
        file_type : (str) data file type
            
        Return
        ---------
        parsed_files : (dict) Python dictionary with dates for keys and lists of associated filenames 
        		      for each unique date 
        		      
        """
        
        if file_path is not None and type(file_path)==str:
            if self.check_path(file_path):
                self.search_path = file_path
            else:
                return
        if file_type is not None and type(file_type)==str:
            self.file_type = file_type

        folder_path = str(self.search_path) + "/**/*" + str(self.file_type)
        print("Searching for files in [{}]...".format(folder_path))
        self.file_list = glob.glob(folder_path, recursive=True)
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
                print("Files matching {}:".format(date[1]))
                for item in data[date[0]]:
                    print("\t" + item)

        # Before returning list, sort files by arranging them in specified order
        parsed_files = self.parse_files(data)

        return parsed_files
    
    
    def set_file_type(self, file_type=None):
        """ Overwrite the file type to read from
        """
        if not file_type:
            print("Warning, no file type specified. Please enter the file type as '.db3' or '.mcap'")
        else:
            self.file_type = file_type
            

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
   
    
    def update_notebook(self, nb_df, date_str, data):
        """ Updates the dataframe with the new data specified by the date and header name
        """
        for header in data:
            # index of date
            date_idx = nb_df.index[nb_df[nb_df.columns[0]] == date_str].tolist()[0]
            nb_df.loc[date_idx, header] = data[header]  # replace date,header index with new value

        return nb_df
        
