"""
 Copyright 2024 Carnegie Mellon University Neuromechatronics Lab

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at https://mozilla.org/MPL/2.0/.

 Contact: Jonathan Shulgach (jshulgac@andrew.cmu.edu)

 This script parses a configuration file and prints the parameters to the console. The parameters can then be used to initialize a DataAgent object.
 """

import os
from data_agent import ArgParser

if __name__ == '__main__':

    # We will use the provided 'config.txt` file which has some basic parameters
    config_file = os.path.abspath(os.getcwd() + '/config.txt')

    # Attempt to load parameters from a local config file. We can also state what the delimiter should be
    pars = ArgParser(file_path=config_file, delimiter='=', verbose=True).scan_file()

    print('\nFinal parameters:')
    for p in pars:
        print(p, pars[p])
