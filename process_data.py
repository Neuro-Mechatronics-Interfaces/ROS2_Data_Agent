"""
# Copyright 2022 Carnegie Mellon University Neuromechatronics Lab
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)
#
# This script searches a specified directory containing bag files and does the following:
#
# 1) Gets all the folders on the path that potentially have task data
#    Ex: 'D:\dev\nml_nhp\ros_workspace-testing\data\220601_145454'
#
# 2) For each folder, see if a file exists AND if the notebook for the animal doesn't already have an entry for that day
#    i. if it already exists, skip to next file
#    ii. if it doesnt exist, do step 3
#
# 3) Read bag file(s) and obtain relevant data
#
# 4) Read and save cursor position (targets, etc.) data to a .txt file in a specified directory
#
# 5) Read and save performance metrics to a .txt file in the same directory
"""

import sys
from data_utils import DataAgent, ArgParser

save = True # Enable if Raptor is online

if __name__ == '__main__':

    # Attempt to load parameters from config file
    data = ArgParser(delimiter='=').scan_file()
    
    # Select user override if parameters provided
    if len(sys.argv) > 1:
        data['CONFIG_PATH'] = sys.argv[1]    
    if len(sys.argv) > 2:
        data['SUBJECT'] = sys.argv[2]        
    if len(sys.argv) > 3:
        data['SEARCH_PATH'] = sys.argv[4]
    if len(sys.argv) > 4:
        data['SAVE_PATH'] = sys.argv[4]

    # Create the DataAgent object, passing the subject name, and search and save directories
    agent = DataAgent(subject=data['SUBJECT'], 
                      search_path=data['SEARCH_PATH'], 
                      save_path=data['SAVE_PATH'], 
                      param_path=data['CONFIG_PATH'],
                      file_type='.mcap',
                     )
    
    # Begin sorting the data files in the search path directory for each reading
    bag_file_list = agent.get_files()
        
    for date, files in bag_file_list.items():
        print("Reading bag files for {}: {}".format(date, files))
        agent.read_files(files=files)  # this step can take a few minutes to an hour...
                      
        #agent.get_forces(date, topic='/robot/command/force', save=save)
        agent.get_states(date, topic='/machine/state', save=save)
        agent.get_targets(date, topic='/environment/target/position', save=save)
        agent.get_cursor_pos(date, '/environment/cursor/position', save=save)
        agent.get_emg(date, '/pico_ros/emg/ch1', save=save)
        agent.get_metrics(date, '/machine/state', verbose=True, save=save)


    # Delete object handle and recreate (temporary solution to closing the accessed .bag file)
    agent = None
    agent = DataAgent(subject=data['SUBJECT'], 
                      search_path=data['SEARCH_PATH'], 
                      save_path=data['SAVE_PATH']
                      ) 
        
    # Finally move the files to the raw_data directory
    for date, files in bag_file_list.items():
        print("Moving all files for {}: {}".format(date, files))
        #new_path = r"R:\NMLShare\raw_data\primate\Forrest"
        agent.move_files_to_data_dir(date, data['NEW_PATH'], verbose=True)
            
            
