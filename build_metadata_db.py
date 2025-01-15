
'''
    Code to build a database of metadata for netcdf or hdf5 files and store in sqlite3 database.
    This database can then be explored through the GUI program metaview.py

    Reads contents of given directory <basedir> and all subdirectories, and kicks off a thread to
    read each file.
    Looks in either the netcdf/hdf5 files (depending on ftype) to find what variables
    are there and stores the metadata for files, coordinates and variables in the database <database_name>.
    In the case of hdf5 files the names of the coordinates [coord1 coord2...] should be given because there is
    no guarantee that the metadata will be adequate to identify which keys are variables and which are
    coordinates.

    In future the -u option will be implemented so that we can check what is already in the database and
    make sure it is up to date

    Usage:
    python build_metadata_db.py, <basedir> <filetype> <database_name> <options -u to update -v=verbose [coord1 coord2 coord3...]>)

    Uses the threading library to make the building of the database multi-threaded. Kicks off one thread per
    file, but limits the number of threads at any time to 10 otherwise OS cannot handle it.

'''

import sys
import os
from read_metadata_thread import *

#-----------------------------------------------------------------------------------
# code to build the database from the metadata of files of type ftype in basedir
# inputs:
#    basedir: the base directory to trawl
#    dbname: the full path and filename of the database
# -----------------------------------------------------------------------------------
def build_db(basedir,dbname):

    # open the database dbname - this will create it if it does not exist
    Read_metadata_thread.con = sqlite3.connect(dbname,check_same_thread=False)
    Read_metadata_thread.cur = Read_metadata_thread.con.cursor()
    # check whether there are any tables
    res = Read_metadata_thread.cur.execute("SELECT name FROM sqlite_master")
    db_exists=False
    table_names=res.fetchall()
    if len(table_names)>0:
        print(table_names)
        db_exists=True
        if Read_metadata_thread.update==False:
            print(dbname, 'already exists')
            exit()
        else:
            res=Read_metadata_thread.cur.execute("""SELECT name FROM Variables""").fetchone()
            if len(res)>0:
                print('update not yet handled')
                exit()


    if db_exists==False:
        create_tables(Read_metadata_thread.cur, verbose=Read_metadata_thread.verbose)

    ndirs=0    

    # now trawl through the directory structure from basedir
    if Read_metadata_thread.verbose:
        print('trawling directory', basedir, 'for', Read_metadata_thread.ftype)
    max_threads=10
    threads=[]
    for dirpath, dirnames, filenames in os.walk(basedir):

        this_dir=Directory(ndirs,dirpath)
        Read_metadata_thread.lock.acquire()
        this_dir.insert_into_database('parent',Read_metadata_thread.cur,Read_metadata_thread.verbose)
        Read_metadata_thread.con.commit()
        ndirs=ndirs+1
        Read_metadata_thread.lock.release()

        for filename in filenames:
            wsplit=filename.split('.')
            if len(wsplit)>1:
                if wsplit[-1] in Read_metadata_thread.allowed_extension:

                    thr = Read_metadata_thread(this_dir,filename)
                    threads.append(thr)
                    thr.start()  # this will call run in Read_metadata_thread

            if len(threads)==max_threads:
                # wait till threads finish
                for x in threads: 
                    x.join()
                threads.clear()

    # wait till threads finish
    for x in threads: 
        x.join()

    for this_var in Read_metadata_thread.variables:
        this_var.insert_into_database('parent',Read_metadata_thread.cur,Read_metadata_thread.verbose)
    # commit the changes
    Read_metadata_thread.con.commit()
    Read_metadata_thread.con.close()

    print(ndirs, 'Directories', Read_metadata_thread.nfiles, 'Files', len(Read_metadata_thread.coords), 'Coords and', len(Read_metadata_thread.variables), 'Variables created')
    if len(Read_metadata_thread.bad_files)>0:
        print('Unable to read the following files')
    for bad in Read_metadata_thread.bad_files:
        print(bad)

# -----------------------------------------------------------------------------------
# main - read the arguments and call build_db
# -----------------------------------------------------------------------------------
def main():

    if len(sys.argv)<4:
        print('usage:', sys.argv[0], '<basedir> <filetype (nc/hdf5)> <database_name> <options eg -u to update, -v=verbose> <[coord1 coord2 coord3...]')
        exit()
    else:
        basedir=sys.argv[1]
        ok=Read_metadata_thread.set_ftype(sys.argv[2]) # this can be 'nc' for netcdf files and 'hdf5' for hdf5 files
        if ok==False:
            exit()

        dbname=sys.argv[3]
        for i in range(4,len(sys.argv)):
            if sys.argv[i]=='-u':
                Read_metadata_thread.update=True
            elif sys.argv[i]=='-v':
                Read_metadata_thread.verbose=True
            else:
                Read_metadata_thread.hdf5_coord_names.append(sys.argv[i])
        if len(Read_metadata_thread.hdf5_coord_names)==0 and Read_metadata_thread.ftype=='hdf5':
            print('coordinate names must be given')
            exit()

        if Read_metadata_thread.update==True:
            print('update not yet implemented')

    build_db(basedir,dbname)



if __name__ == '__main__':
    main()
