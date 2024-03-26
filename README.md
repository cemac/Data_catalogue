# Data_catalogue
A cataloguer of netcdf/hdf5 files in a directory. Includes code to build a sqlite3 database of metadata and code to run a GUI to explore the database.
build_metadata_db.py contains the code to build the database and is run as 
python build_metadata_db.py <indir> <ftype> <dbpathname> -v
where <indir> is the directory you want to catalogue and dbpathname is the full pathname of the database file to create

metaview.py contains the code to run a GUI to display the contents of the database with various filter options ansd is run as
python metaview.py <dbpathname> -v [coord1 coord2...]
where coord1 coord2... are the names of the coordinates that you wish to appear on the GUI as filters, eg. longitude latitude level time
