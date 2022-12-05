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
import pandas as pd
from data_utils import DataAgent


if __name__ == '__main__':

    search_dir = r'D:\dev\nml_nhp\ros_workspace-testing\data\awaiting_process'
    agent = DataAgent(search_dir=search_dir, subject='Forrest', verbose=True)
    
    # Pass directory to where .yaml files are for use with metadata collection
    config_dir = r'D:\dev\nml_nhp\ros_workspace-testing\config'
    agent.add_param_path(config_dir)

    # Check that the directory is valid, continue if so. DataAgent object saves file path
    if agent.check_path(search_dir):
        bag_file_list = agent.get_files()
        
        for date, files in bag_file_list.items():
            print("Reading bag files for {}: {}".format(date, files))
            agent.read_files(files=files)  # this step can take a few minutes to an hour...
                       
            agent.save_cursor_pos(date)
            agent.save_targets(date)
            agent.save_states(date)
            agent.save_metrics(date, get_metrics=True)
