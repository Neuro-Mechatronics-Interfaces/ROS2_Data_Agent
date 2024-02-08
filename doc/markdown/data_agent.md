

The `DataAgent` class is a class that searches a directory for matching file types and extracts file data with keyword inputs.
 
The full parameter inputs are:
- subject:     (str) Name of the subject to identify data with
- search_path: (str) Directory for raw data files
- save_path:   (str) Directory for the generated data files
- param_path:  (str) directory for the task's .yaml config files for metadata collection
- file_type:   (str) file type to search for in all folders from root directory
- verbose:     (bool) enable/disable verbose output
 