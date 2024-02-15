"""
 Copyright 2024 Carnegie Mellon University Neuromechatronics Lab

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at https://mozilla.org/MPL/2.0/.

 Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

 This script converts ROS2 bag files from an experiment into a mat file into the same data directory
 """

from data_agent import DataAgent

if __name__ == '__main__':

    # Set up some parameters
    SUBJECT   = 'TestUser'
    data_path = '/path/to/data'
    date      = '12/20/23'
    file_type = '.mcap'

    # Create the DataAgent object, pass in the subject, search path, and file type arguments
    agent = DataAgent(SUBJECT, data_path)    

    # Search for bag files in the search_path directory already specified by the 'search_path' argument
    # when the object was initialized. It's going to create a list of dictionaries with metadata for 
    # each folder found. Although... we don't want to read ALL the bag files that could potentially be 
    # in the search path, so we will specify further with a date we pass as an argument.
    file_list = agent.search_for_files(file_type=file_type, date_tag=date)
        
    # The resulting file list will have all bag files detected in that search path on that particular 
    # day. There might be multiple trial recordings on the same day as well, so we can loop through and 
    # save each one with an index at the end of the file name
    for i, rec in enumerate(file_list):

        print("Reading bag files for {}, tag:{}:".format(rec['date'], rec['file_tag']))
        agent.read_bag(rec['files'], combine=True)  # this step can take a few minutes to an hour...

        # Save bag file data for that trial into an .mat file in the same folder
        file_name = rec['folder_path'] + "/{}_{}_{:02d}_{:02d}_rosbag_{}".format(SUBJECT, rec['year'], rec['month'], rec['day'], str(i))

        agent.save_data(file_name, 'mat')
        print("done, next...")
            
    # And we are done!
    print("all done!!")        
            
            
            
