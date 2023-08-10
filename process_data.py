"""
# Copyright 2022 Carnegie Mellon University Neuromechatronics Lab
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)
#
# This script should go through each day in the ros workspace and do the following:
#
# 1) Get all the folders on the local machine that potentially have task data
#    Ex: 'D:\dev\nml_nhp\ros_workspace-testing\data\220601_145454'
#
# 2) For each folder, see if a file exists AND if the notebook for the animal doesn't already have an entry for that day
#    i. if it already exists, skip to next file
#    ii. if it doesnt exist, do step 3
#
# 3) Read bag file(s) and obtain relevant data
#
# 4) Read and save cursor position data to a .txt file to specified directory
#
# 5) Read and save performance metrics to a .txt file to same directory
"""

import sys
import os
from pathlib import Path
from data_utils import DataAgent, ArgParser

save = True # Enable if Raptor is online

if __name__ == '__main__':

    #default parameters 
    #subject = 'Forrest'
    #raw_data_path = '/run/user/1000/gvfs/smb-share:server=raptor.ni.cmu.edu,share=nml/NMLShare/raw_data/primate'    
    #raw_data_path = 'D:/dev/nml_nhp/ros_workspace-testing/data' 
    #save_data_path = 'R:/NMLShare/generated_data/primate/Cursor_Task'
    
    # Attempt to load parameters from config file
    data = ArgParser().scan_file()
    
    # Select user override if parameters provided
    if len(sys.argv) > 1:
        data['CONFIG_PATH'] = sys.argv[1]
    
    if len(sys.argv) > 2:
        data['SUBJECT'] = sys.argv[2]
        
    if len(sys.argv) > 3:
        data['SEARCH_PATH'] = sys.argv[4]

    #search_dir = Path(os.path.join(data['SEARCH_PATH'], "{}".format(data['SUBJECT']), "awaiting_process"))
    search_dir = data['SEARCH_PATH']

    if len(sys.argv) > 4:
        data['SAVE_PATH'] = sys.argv[4]

    # Create the DataAgent object, passing the subject name, and search and save directories
    agent = DataAgent(subject=data['SUBJECT'], search_dir=search_dir, save_path=data['SAVE_PATH'])
    
    # Include directory to where .yaml config files are for use with metadata collection
    #config_dir = r'/home/nml/nml_nhp/nml_plexon-ros_workspace/config'
    #config_path = r'D:/dev/nml_nhp/ros_workspace-testing/config'
    agent.add_param_path(data['CONFIG_PATH'])

    # Check that the directory is valid, continue if so. DataAgent object saves file path
    if agent.check_path(search_dir):
        bag_file_list = agent.get_files()
        
        for date, files in bag_file_list.items():
            #print("Reading bag files for {}: {}".format(date, files))
            #agent.read_files(files=files)  # this step can take a few minutes to an hour...
                      
            #agent.get_forces(date, save=save)
            #agent.get_cursor_pos(date, save=save)
            #agent.get_targets(date, save=save)
            #agent.get_states(date, save=save)
            #agent.get_metrics(date, verbose=True, save=save)
            
            print("Moving bag files for {}: {}".format(date, files))
            new_path = r"R:\NMLShare\raw_data\primate\Forrest"
            agent.move_files_to_data_dir(date, new_path, verbose=True)
            
            
