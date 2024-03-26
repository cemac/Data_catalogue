# Data_catalogue
A cataloguer of netcdf/hdf5 files in a directory. Includes code to build a sqlite3 database of metadata and code to run a GUI to explore the database.

build_metadata_db.py contains the code to build the database and is run as 
python build_metadata_db.py indir ftype dbpathname -v
where indir is the directory you want to catalogue, ftype is nc for netcdf files or hdf5 for hdf5 files and dbpathname is the full pathname of the database file to create.
If you want to run this on directories of hdf5 files you need to specify the names of the coordinates as it is not always possible to determine that
In future I will add the ability to update the database.

metaview.py contains the code to run a GUI to display the contents of the database with various filter options ansd is run as
python metaview.py dbpathname -v [coord1 coord2...]
where coord1 coord2... are the names of the coordinates that you wish to appear on the GUI as filters, eg. longitude latitude level time. Still to be resolved - a know problem where a segmentation fault occurs when the GUI is closed.

db_functions.py contains class definitions to hold metadata extracted from file and to insert the data into the database and to retrieve the data from the database. These functions are used by build_metadata_db.py and metaview.py
