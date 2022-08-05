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
# Ex: 'D:\dev\nml_nhp\ros_workspace-testing\data\220601_145454'
#
# 2) For each folder, see if a file exists AND if the notebook for the animal doesn't already have an entry for that day
#    i. if it already exists, skip to next file
#    ii. if it doesnt exist, do step 3
#
# 3) Read bag file(s) and obtain relevant data
#
# 4) Read and save cursor positiondata to a .txt file in the local directory
"""

import sys
import pandas as pd
import numpy as np
from data_utils import DataAgent


if __name__ == '__main__':

    agent = DataAgent()

    # Check that the directory is valid, continue if so. DataAgent object saves file path
    if agent.check_path(sys.argv[1]):
        bag_file_list = agent.get_files()
        
    for date, files in bag_file_list.items():
        print("Reading bag files for {}: {}".format(date, files))
        agent.read_files(files=files)  # this step can take a few minutes to an hour...
                       
        #>>> temp_df = pd.DataFrame(agent._data[0].records) # For everything
        temp_df = agent.get_topic_data('/cursor/position') # For just cursor position
        file_name = date.replace('/','_') + '_CURSOR_POS.txt'
        temp_df.to_csv(file_name)