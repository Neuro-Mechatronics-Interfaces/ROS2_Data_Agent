"""
 Copyright 2024 Carnegie Mellon University Neuromechatronics Lab

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at https://mozilla.org/MPL/2.0/.

 Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

 This script converts ROS2 bag files from an experiment into a mat file into the same data directory
 """
import os
import argparse
import pandas as pd
from datetime import date
from data_agent import DataAgent
from data_agent import ArgParser

if __name__ == '__main__':

    # STEP 1. Initialize parameters
    # Set up some parameters
    parser = argparse.ArgumentParser(description='Convert ROS2 bag files from an experiment into a mat file into specified data directory')
    parser.add_argument('--subject', '-subject', type=str, default=None, help='Name of the subject (ex: TestUser)')
    parser.add_argument('--search_path', '-search_path', type=str, default=None, help="Path to the data directory (default: same local directory as 'process_data.py' file")
    parser.add_argument('--date', '-date', type=str, default=None, help="Date of the experiment (ex: '12/20/23')")
    parser.add_argument('--config_file', '-config_file', type=str, default=None, help="Path to the configuration file (default: '/path/to/config.yaml')")
    parser.add_argument('--file_type', '-file_type', type=str, default='.mcap', help="File type to search for (default: '.mcap')")
    parser.add_argument("-v", "--verbose", "-verbose", type=bool, default=False, help="Print out more information (default: False)")
    parser.add_argument('--save_path', '-save_path', type=str, default=None, help="Path to the directory where the .mat files will be saved (default: same as 'search_path')")
    parser.add_argument('--param_path', '-param_path', type=str, default=None, help="Path to the directory where .yaml file with parameters is located (default: same as 'search_path')")
    parser.add_argument('--notebook_path', '-notebook_path', type=str, default=None, help="Path to the subject digital notebook" )
    parser.add_argument('--notebook_sheet', '-notebook_sheet', type=str, default=None, help="Name of the sheet in the notebook to update")
    args = parser.parse_args()

    # Attempt to load parameters from a local config file
    #pars = ArgParser(file_path=os.path.join(os.getcwd(), 'config_forrest.txt'), delimiter='=').scan_file()
    pars = ArgParser(file_path=os.path.abspath(args.config_file), delimiter='=', verbose=True).scan_file()

    #print(pars)
    
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

    if args.notebook_path is None:
        if 'notebook_path' in pars:
            notebook_path = pars['notebook_path']
        else:
            notebook_path = None
    else:
        notebook_path = args.notebook_path

    if args.notebook_sheet is None:
        if 'notebook_sheet' in pars:
            notebook_sheet = pars['notebook_sheet']
        else:
            notebook_sheet = None
    else:
        notebook_sheet = args.notebook_sheet

    # STEP 2. Loading data
    # Create the DataAgent object.
    agent = DataAgent(subject=subject, search_path=search_path, save_path=save_path, param_path=param_path,
                      file_type=args.file_type, verbose=verbose)

    # Search for bag files
    assert agent.search_path is not None
    file_list = agent.search_for_files(date_tag=date)
    print(file_list)
    for i, rec in enumerate(file_list):
        print("Reading bag files for {}, tag:{}:".format(rec['date'], rec['file_tag']))
        agent.read_bag(rec['files'], combine=True)  # this step can take a few minutes to an hour...

        # Save bag file data for that trial into an .mat file in the same folder
        file_name = rec['folder_path'] + "/{}_{}_{:02d}_{:02d}_rosbag_A_{}".format(subject, rec['year'], rec['month'], rec['day'], str(i))
        agent.save_data(file_name, 'mat')

    # ================ Uncomment below if you want to save the dataframe object and load it later =================
    # df = agent._data
    # df.to_csv('df.csv')  # Save dataframe object to current directory
    # print("Loading notebook data")
    # df = pd.read_csv("df.csv", index_col=[0], low_memory=False)
    # =============================================================================================================

    # STEP 3. Analysis
    # Now that the data has been saved, we can calculate the metrics and update the subject notebook
    metrics = agent.get_metrics(date, state_topic='/machine/state', display_metrics=True, save=True, overwrite=True, return_metrics=True)

    # STEP 4. Update the subject notebook
    # Load the workbook. We also want to keep the original formatting styles the workbook has
    #agent.load_notebook(notebook_path)

    # Find the row and column indices of the metrics names of interest for the correct date
    #col_indices = agent.find_notebook_columns(metrics.keys(), notebook_sheet)
    #row_index = agent.find_notebook_row(date, notebook_sheet)

    # Update the columns with the info from metrics, where the column index is the same as the key in metrics
    #print("Updating notebook with metrics...")
    #for key in metrics.keys():
    #    if key in col_indices:
    #        agent.write_to_sheet(notebook_sheet, row_index, col_indices[key], metrics[key])

    # Save updated workbook
    #agent.save_notebook(notebook_path, overwrite=True)
    #print("Finished saving to notebook")






