"""
# Copyright 2022 Carnegie Mellon University Neuromechatronics Lab
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

# This script should go through each day in the ros workspace and do the following:

# 1) Get all the folders on the local machine that potentially have task data
# Ex: 'D:\dev\nml_nhp\ros_workspace-testing\data\220601_145454'
#
# 2) For each folder, see if a file exists AND if the notebook for the animal doesn't already have an entry for that day
#    i. if it already exists, skip to next file
#    ii. if it doesnt exist, do step 3
#
# 3) read bag file and obtain relevant data
#
# 4) Update animal notebook (located on google drive, or configurable path)
#   i. Total number of trials
#   ii. success percentage
#   iii. water dispensed
#
# 5) Generate overlayed figures for total number of trials for the last 30 days, the success percentage, and
#    the 10-day success average
#
#    TO-DO: make adjustment to reward dispenser node so that the amount dispensed is recorded from the start of the task
"""

import sys
from data_utils import DataAgent, BagParser

if __name__ == '__main__':

    # Make agent and get locations of all bag files
    agent = DataAgent(verbose=False)
    bag_file_list = None
    if agent.check_path(sys.argv):
        bag_file_list = agent.get_files()

    # If you want to directly update a spreadsheet from teh data collected with the bag files, define notebook info
    nb_headers = agent.set_notebook_headers(sys.argv, output=True)
    # TO-DO: change to direct path instead of animal
    # df = agent.get_notebook_data('G:/Shared drives/NML_NHP/Monkey Training Records/Forrest Backup')
    df = agent.get_notebook_data('Forrest Backup')

    # print(df.columns)

    for date, files in bag_file_list.items():
        start = True
        if df is not None:
            skip_header = agent.check_notebook_entry(df, date, nb_headers)
            if skip_header is None:
                print("No notebook field selected, stopping...")
                break

            if not False in skip_header:
                print("Date full, skipping {}".format(date))
                start = False

        if start:
            # Read all bag files for date and collect
            print("Reading bag files for {}: {}".format(date, files))
            parser = BagParser(files=files)  # this step can take a few minutes...
            # time.sleep(0.5)

            # Get total number of trials (success + incomplete states)
            print("getting trial percentages")
            # print("Trial data: \n N Trials: 6000\nSuccess: 2000\nFailure: 4000")
            [N_trials, N_success, N_failure] = parser.get_trial_performance()

            # Update notebook with new data indexed by working date and header type.
            # NOTE: This works... but it removes all existing conditioning and colors on the spreadsheets
            # updated_nb = agent.update_notebook(df, date, parser_data)
            # nb_path = r'G:\Shared drives\NML_NHP\Monkey Training Records\Forrest Backup 2.xlsx'
            # updated_nb.to_excel(nb_path)

            # Choosing not to overwrite existing workbook in order to keep formatting and visual basic viewing options, instead
            # we will save processed numbers to new worksheet and copy-paste data to original
            print("Writing values to text file")
            with open("MyFile.txt", "a") as file1:
                file1.write(date + ",N:" + str(6000) + ",Success:" + str(3000) + ",Failure:" + str(3000) + "\n")
                file1.close()
