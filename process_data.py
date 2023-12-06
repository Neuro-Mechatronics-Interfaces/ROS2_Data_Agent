"""
 Copyright 2023 Carnegie Mellon University Neuromechatronics Lab

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at https://mozilla.org/MPL/2.0/.

 Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

 This script demonstrates the capabilities of the DataAgent class. In general, it looks for bag files 
 in a specified directory, collects and saves text files with key topic and matric data to a saving 
 directory, creates relevant subject and training day subfolders, and moves files from a local 
 path to a data server.

"""

import sys
import time
from data_utils import DataAgent, ArgParser

save = True 
verbose = True

if __name__ == '__main__':

    # Attempt to load parameters from a local config file. We can also pass in any system arguments
    pars = ArgParser(delimiter='=', args=sys.argv).scan_file()

    # Create the DataAgent object. We can define the subject name, search, and save directories here, 
    # but in this case we already read them from the config.txt file using ArgParser, so we can pass 
    # the dictionary values as the constructor arguments
    agent = DataAgent(subject=pars['subject'],     search_path=pars['search_path'],   
                      save_path=pars['save_path'], param_path=pars['param_path'],
                      file_type='.mcap', verbose=verbose )    

    # Begin sorting the data files in the 'search_path' directory for each reading
    bag_file_list = agent.search_for_files()
    for date, files in bag_file_list.items():
        
        # We will read the actual bag file(s) for the first time
        print("Reading bag files for {}:".format(date))
        agent.read_bag(files)  # this step can take a few minutes to an hour...
                      
        # Let's grab some topics and performance metric data
        agent.get_states(date, topic='/machine/state', save=save)
        agent.get_targets(date, topic='/environment/target/position', save=save)
        agent.get_cursor_pos(date, '/environment/cursor/position', save=save)
        agent.get_metrics(date, '/machine/state', save=save)
        
        # Some more methods below that can save specific topic types
        #agent.get_emg(date, '/pico_ros/emg/ch1', save=save)
        #agent.get_forces(date, topic='/robot/command/force', save=save)
        
        #TO-DO: generic topic data extraction and creating a text file to read later
        #      ex: agent.get_topic(topic='/some/topic', save=True, tag=date, filename='SomeTopic.txt')  
        
        # When we're done analyzing the local files, we can move them to a data server. Instead of 
        # moving each bag file separately, we will instead move & rename the entire training day 
        # folder. Set the move_parent_folder argument to true tells the agent that whichever parent 
        # folder the file belongs to will be moved and all the contents with it. We can also pass in 
        # a 'date' tag to finish creating the saving directory
        #print("Moving all files for {}".format(date))
        #agent.transfer_data(files=files, new_path=pars['new_path'], tag=date, move_parent_folder=True)
        
            
    # And we are done!
            
