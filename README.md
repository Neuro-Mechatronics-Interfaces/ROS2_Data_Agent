# ROS2_Data_Agent #
Code for a multipurpose file explorer specializing in reading ROS2 topic data from '.bag' or '.db3' files
--
#### (Note: Only tested in Windows 10 with ROS2 Galactic)


## Prerequisites
- [Ros2 Galactic Windows 10](https://docs.ros.org/en/galactic/Installation/Windows-Install-Binary.html)
- Python 3.8 with packages pandas, numpy, openpyxl (Use the package manager [pip](https://pip.pypa.io/en/stable/) to install).

```bash
pip install pandas numpy openpyxl bag
```
- The `nml_bag` package from Andrew Whitmore for reading bag files created from ROS2. Clone the repository and run the installatiion file.
```bash
git clone https://github.com/ricmua/nml_bag.git
pip install /path/to/nml_bag
```

### Usage ###
1. Navigate to directory containing `main.py`
2. Run file with string containing root directory as 1st argument, file type string as the second argument, and data field string as the third
```bash
python main.py 'D:/dev/nml_nhp/data' '.db3' 'CORRECT TRIALS'
```
If log files are detected, the results of the data collection are stored in a `data` folder generated in the local directory as a text file.

