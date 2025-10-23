# Data_catalogue
A cataloguer of netcdf/hdf5 files in a directory. Includes code to build a sqlite3 database of metadata and code to run a GUI using tkinter to explore the database.
The code works with a conda environment created using the environment.yml file.

build_metadata_db.py contains the code to build the database and is run as:

python build_metadata_db.py indir ftype dbpathname -v

where indir is the directory you want to catalogue, ftype is nc for netcdf files or hdf5 for hdf5 files and dbpathname is the full pathname of the database file to create.
If you want to run this on directories of hdf5 files you need to specify the names of the coordinates as it is not always possible to determine that from the metadata itself.

metaview.py contains the code to run a GUI to display the contents of the database with various filter options and is run as:

python metaview.py dbpathname -v [coord1 coord2...]

where coord1 coord2... are the names of the coordinates that you wish to appear on the GUI as filters, eg. longitude latitude level time. Still to be resolved - a segmentation fault occasionally occurs when the GUI is closed.

db_functions.py contains class definitions to hold metadata extracted from a file and to insert the data into the database and to retrieve the data from the database. These functions are used by read_metadata_db.py and metaview.py. 

read_metadata_thread.py contains the code to read a single file and add the metadata to a database. This is used by build_metadata_db.py which kicks off a thread for each file. It is also used by test_build_metadata.py which builds a database but on just the one file given on the command line and is used just for testing.

# User Guide
The initial screen appears as:
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/initial_screen.png)

To view all the variables in the database, click the 'Search' button while the 'Variable' entry is still set to '*':
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/all_variables.png)

If you are just interested in a particular variable you can select that variable from the drop down list by clicking on 'Variable':
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/choose_variable.png)

When the variable is selected, previous results are cleared. You then need to click 'Search' to view the variable selected:
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/single_var_searched.png)

If the database contains metadata from multiple directories, you may select a specific directory from which to view information using the drop down menu by clicking on 'Directory':

![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/select_directory.png)

Likewise a filename or part filename may be entered in the 'Filename' entry box to only view variables in certain files.
You may also set ranges of latitudes, longitudes, times or pressure levels to see what variables cover those ranges. Each time you change a filter you will need to click the 'Search' button to view the results.

To view the attributes of a particular variable, click on the name of the variable in the results panel and a pop-up window appears:
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/var_attributes.png)

To view the coordinates relating to a dimension of a particular variable, click on the dimension of the variable in the results panel and a pop-up window appears. If the coordinate is the same in all files in which this variable occurs and the coordinate values are evenly spaced (eg. latitude), the pop-up window shows the range of that coordinate:

![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/lat_coord.png)

If the coordinate has discrete values (eg. pressure levels), all the values appear in the pop-up window:
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/discrete_value_coord.png)

If the coordinate varies for each file (eg different times in each file), the pop-up window contains information about the coordinate values in each file:

![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/time_multi_coord.png)

To view the information about the files in which a particular variable occurs, click on the word 'files' on the line in the results panel relating to the variable. A list of files, their creation and modification dates and whether they are symbolic links appears in a pop-up window.
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/files.png)

To view the global attributes of a particular file, click on the line of the file displayed in the above pop-up window and a second pop-up window appears containing the global attributes:
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/file_attribs.png)



# Database design
![ALT TEXT](https://github.com/cemac/Data_catalogue/blob/main/images/database_design.png)







