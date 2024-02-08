"""
 Copyright 2024 Carnegie Mellon University Neuromechatronics Lab

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at https://mozilla.org/MPL/2.0/.

 Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

 This script converts ROS2 bag files from an experiment into a mat file into the same data directory
 """
import os
from datetime import date
import argparse
from data_utils import DataAgent
from data_utils import ArgParser

if __name__ == '__main__':

    # Set up some parameters
    parser = argparse.ArgumentParser(description='Convert ROS2 bag files from an experiment into a mat file into specified data directory')
    parser.add_argument('--subject', '-subject', type=str, default=None, help='Name of the subject (ex: TestUser)')
    parser.add_argument('--search_path', '-search_path', type=str, default=None, help="Path to the data directory (default: same local directory as 'rosbag_to_mat.py' file")
    parser.add_argument('--date', '-date', type=str, default=None, help="Date of the experiment (ex: '12/20/23')")
    parser.add_argument('--config_file', '-config_file', type=str, default=None, help="Path to the configuration file (default: '/path/to/config.yaml')")
    parser.add_argument('--file_type', '-file_type', type=str, default='.mcap', help="File type to search for (default: '.mcap')")
    parser.add_argument("-v", "--verbose", "-verbose", type=bool, default=None, help="Print out more information (default: False)")
    parser.add_argument('--save_path', '-save_path', type=str, default=None, help="Path to the directory where the .mat files will be saved (default: same as 'search_path')")
    parser.add_argument('--param_path', '-param_path', type=str, default=None, help="Path to the directory where .yaml file with parameters is located (default: same as 'search_path')")
    args = parser.parse_args()

    # Attempt to load parameters from a local config file
    pars = ArgParser(file_path=args.config_file, delimiter='=', verbose=True).scan_file()
    print(pars)

    if args.subject is None:
        # Check is 'subject' key exists in python dict pars
        if 'subject' in pars:
            subject = pars['subject']
        else:
            raise ValueError("Subject name not found in config file or as an argument")
    else:
        subject = args.subject

    if args.search_path is None:
        # Check is 'search_path' key exists in python dict pars
        if 'search_path' in pars:
            search_path = pars['search_path']
        else:
            search_path = os.path.dirname(os.path.abspath(__file__))
    else:
        search_path = args.search_path

    if args.date is None:
        if 'date' in pars:
            date = pars['date']
        else:
            date = date.today().strftime("%m/%d/%y")
    else:
        date = args.date

    if args.save_path is None:
        if 'save_path' in pars:
            save_path = pars['save_path']
        else:
            save_path = search_path
    else:
        save_path = args.save_path

    if args.file_type is None:
        if 'file_type' in pars:
            file_type = pars['file_type']
        else:
            file_type = '.mcap'
    else:
        file_type = args.file_type

    if args.verbose is None:
        if 'verbose' in pars:
            verbose = pars['verbose']
        else:
            verbose = False
    else:
        verbose = args.verbose

    if args.param_path is None:
        if 'param_path' in pars:
            param_path = pars['param_path']
        else:
            param_path = None
    else:
        param_path = args.param_path

    # Create the DataAgent object. We can define the subject name, search, and save directories here,
    # but in this case we already read them from the config.txt file using ArgParser, so we can pass
    # the dictionary values as the constructor arguments
    #agent = DataAgent(subject=subject, search_path=search_path, save_path=save_path, param_path=param_path,
    #                  file_type=args.file_type, verbose=verbose)

    # Search for bag files in the search_path directory already specified by the 'search_path' argument
    # when the object was initialized. It's going to create a list of dictionaries with metadata for
    # each folder found. Although... we don't want to read ALL the bag files that could potentially be
    # in the search path, so we will specify further with a date we pass as an argument
    #file_list = agent.search_for_files(date_tag=date)

    # The resulting file list will have all bag files detected in that search path on that particular
    # day. There might be multiple trial recordings on the same day as well, so we can loop through and
    # save each one with an index at the end of the file name
    #for i, rec in enumerate(file_list):
    #    print("Reading bag files for {}, tag:{}:".format(rec['date'], rec['file_tag']))
    #    agent.read_bag(rec['files'], combine=True)  # this step can take a few minutes to an hour...

        # Save bag file data for that trial into an .mat file in the same folder
    #    file_name = rec['folder_path'] + "/{}_{}_{:02d}_{:02d}_rosbag_A_{}".format(SUBJECT, rec['year'], rec['month'],
    #                                                                               rec['day'], str(i))

    #    agent.save_data(file_name, 'mat')
    #    print("done, next...")

    # And we are done!
    #print("all done!!")



