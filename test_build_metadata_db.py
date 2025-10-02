#--------------------------------------------------------------
# used to test the reading of a single file to get the metadata
# and storing it in a database so we don't take long to build a database
# from a whole directory of files
# This works in the same way as the real build_metadata_db.py.
#---------------------------------------------------------------
import sys
import os
from read_metadata_thread import *

def main():

    if len(sys.argv)<2:
        print('usage:', sys.argv[0], '<filepath> <-v=verbose> <[coord1 coord2 coord3...]')
        exit()
    else:
        verbose=False
        
        filepath=sys.argv[1]
        wsplit=filepath.split('/')
        filename=wsplit[-1]
        dirpath=filepath.split('/'+filename)[0]
        print('dirpath',dirpath)
        print('filename', filename)
        wsplit=filename.split('.')
        ok=Read_metadata_thread.set_ftype(wsplit[-1]) # this can be 'nc' for netcdf files and 'hdf5' for hdf5 files
        if ok==False:
            exit()

        for i in range(2,len(sys.argv)):
            if sys.argv[i]=='-v':
                Read_metadata_thread.verbose=True
            else:
                Read_metadata_thread.hdf5_coord_names.append(sys.argv[i])
        if len(Read_metadata_thread.hdf5_coord_names)==0 and Read_metadata_thread.ftype=='hdf5':
            print('coordinate names must be given')
            exit()
 
    # use filename with .nc removed and .db added
    wsplit=filename.split('.'+Read_metadata_thread.ftype)
    dbname='./Databases/'+wsplit[0]+'.db'          
    Read_metadata_thread.con = sqlite3.connect(dbname,check_same_thread=False)
    Read_metadata_thread.cur = Read_metadata_thread.con.cursor()
    # check whether there are any tables
    res = Read_metadata_thread.cur.execute("SELECT name FROM sqlite_master")
    db_exists=False
    table_names=res.fetchall()
    if len(table_names)>0:
        print(table_names)
        db_exists=True
        print(dbname, 'already exists')
        exit()


    if db_exists==False:
        create_tables(Read_metadata_thread.cur, verbose=verbose)
    this_dir=Directory(0,dirpath)
    Read_metadata_thread.lock.acquire()
    this_dir.insert_into_database('parent',Read_metadata_thread.cur,Read_metadata_thread.verbose)
    Read_metadata_thread.con.commit()
    Read_metadata_thread.lock.release()
        
    thr = Read_metadata_thread(this_dir,filename)
    thr.start()  # this will call run in Read_metadata_thread

    thr.join()    
    # insert all the variables into the database    
    for this_var in Read_metadata_thread.variables:
        this_var.insert_into_database('parent',Read_metadata_thread.cur,Read_metadata_thread.verbose)
    # commit the changes
    Read_metadata_thread.con.commit()
    Read_metadata_thread.con.close()
    
if __name__ == '__main__':
    main()
