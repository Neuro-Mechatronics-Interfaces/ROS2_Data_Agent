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
# 4) (Optional) Update animal notebook (located on google drive, or configurable path)
#   i. Total number of trials
#   ii. success percentage
#   iii. water dispensed
#
# 5) (Optional) Generate overlayed figures for total number of trials for the last 30 days, the success percentage, and
#    the 10-day success average
#
#    TO-DO: make adjustment to reward dispenser node so that the amount dispensed is recorded from the start of the task
"""

import sys
from data_utils import DataAgent

if __name__ == '__main__':
    file_path = None
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    if len(sys.argv) > 2:
        file_type = sys.argv[2]

    # Make agent and get locations of all bag files
    agent = DataAgent(verbose=False)
    bag_file_list = None

    # Check that the directory is valid, continue if so. DataAgent object saves file path
    if agent.check_path(file_path):
        bag_file_list = agent.get_files()


        # If you want to directly update a spreadsheet from the data collected with the bag files, define notebook info
        df = None
        if len(sys.argv) > 3:
            nb_headers = agent.set_notebook_headers(sys.argv[3], output=True)
            # TO-DO: change to direct path instead of animal
            # df = agent.get_notebook_data('G:/Shared drives/NML_NHP/Monkey Training Records/Forrest Backup')
            #df = agent.get_notebook_data('Forrest Backup')

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
                agent.read_files(files=files)  # this step can take a few minutes...
                
                # Create Dataframe object containing information from specific topic using the 'get_topic_data()' method--
                #target_pos_df = agent.get_topic_data('/target/position')

                # --or use custom methods
                # Ex: Get total number of trials and percent success rate (success + incomplete states)
                print("Getting trial performance statistics...")
                # print("Trial data: \n N Trials: 6000\nSuccess: 2000\nFailure: 4000")
                [N_trials, N_success, N_failure] = agent.get_trial_performance()

                # Update notebook with new data indexed by working date and header type.
                # NOTE: This works... but it removes all existing conditioning and colors on the spreadsheets
                # if df is not None:
                #     updated_nb = agent.update_notebook(df, date, parser_data)
                #     nb_path = r'G:\Shared drives\NML_NHP\Monkey Training Records\Forrest Backup 2.xlsx'
                #     updated_nb.to_excel(nb_path)

                # Choosing not to overwrite existing workbook in order to keep formatting and visual basic viewing options, instead
                # we will save processed numbers to new worksheet and copy-paste data to original
                print("Writing values to text file")
                file_name = "Performance_Metrics.txt"
                with open(file_name, "a") as f:
                    f.write(date + ",N:" + str(N_trials) + ",Success:" + str(N_success) + ",Failure:" + str(N_failure) + "\n")
                    #f.write(date + ",N:" + str(6000) + ",Success:" + str(3000) + ",Failure:" + str(3000) + "\n")
                    f.close()
