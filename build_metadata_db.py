
'''
    Code to build a database of metadata for netcdf or hdf5 files and store in sqlite3 database.
    This database can then be explored through the GUI program metaview.py

    Reads contents of given directory <basedir> and all subdirectories, 
    Looks in either the netcdf/hdf5 files (depending on ftype) to find what variables
    are there and stores the metadata for files, coordinates and variables in the database <database_name>.
    In the case of hdf5 files the names of the coordinates [coord1 coord2...] should be given because there is
    no guarantee that the metadata will be adequate to identify which keys are variables and which are
    coordinates.

    In future the -u option will be implemented so that we can check what is already in the database and
    make sure it is up to date

    Usage:
    python build_metadata_db.py, <basedir> <filetype> <database_name> <options -u to update -v=verbose [coord1 coord2 coord3...]>)

'''

import sys
import os
import warnings
import numpy as np
import datetime as dt
import sqlite3
import pdb
from netCDF4 import Dataset, num2date, date2num
from db_functions import *

#-----------------------------------------------------------------------------------
# Adds to database the metadata from one netcdf file.
# This will create the entry for the file in the Files table and entries for any coords in the Coords table
# Any variables will be held in the variables list but cannot be added to the database until we have read
# all files and set up all the cids and fids.
#
# inputs:
#    dirpath - directory of file
#    filename - fiename of file
#    fid - is the id for the file we are reading
#    coords - a list of the coords we have created so far
#    variables - a list of the variables we have created so far
#    cur - a cursor to the database
#    update - allow updates to database - NOT YET IMPLEMENTED
#    verbose - control printing
# returns:
#    ok=True/False - indicates whether we could read the file
#-----------------------------------------------------------------------------------
def read_netcdf(dirpath, filename, fid, coords, variables, cur, update, verbose):
    ok=False
    filepath=get_filepath(dirpath,filename)

    try:
        if verbose:
            print('reading', filepath)
        data=Dataset(filepath, "r", format="NETCDF4")
        ok=True

    except OSError as err:
        warnings.warn('cannot read file {filename}, error={err}'.format(filename=filename, err=err), UserWarning)
        return ok

    this_file=File_metadata(fid, dirpath, filename)
    # get the global attributes
    for attrname in data.ncattrs():
        value=getattr(data, attrname)
        this_file.add_attribute(attrname,value)

    this_file.insert_into_database(cur,verbose) # we can insert the file entry into the database

    ncoords=len(coords)
    exisiting_coord_names=np.asarray([coord.name for coord in coords])
    # get the coords from this file
    this_cixes=[]
    this_dimnames=[]
    nvars=len(variables)
    for d in data.dimensions:
        # read the information about this coordinate by creating a coordinate instance
        this_coord=Coord_metadata(ncoords, d, data[d][:])
        for attrname in data[d].ncattrs():
            value=getattr(data[d], attrname)
            this_coord.add_attribute(attrname,value)
        # do we already have this coordinate
        matches=False
        for c in range(ncoords):
            matches=coords[c].matches_coord(this_coord)
            if matches:
                break
        if matches==False:
            # we don't have it so append it to coords list and store it in the database
            coords.append(this_coord)
            this_cixes.append(ncoords)
            ncoords=ncoords+1
            this_coord.insert_into_database(cur,verbose)
        else:
            this_cixes.append(c)
            if verbose:
                print('matching coordinate exists', d, c, coords[c].cid)
        this_dimnames.append(d)

    this_dimnames=np.asarray(this_dimnames)
    # get the variable names that are not in dimensions
    for v in data.variables:
        is_dim=False
        for d in data.dimensions:
            if v==d:
                is_dim=True
                break
        if is_dim==False:
            # this is a proper variable so create a variable instance
            ndims=len(data[v].dimensions)
            this_var=Variable_metadata(nvars,v,ndims)
            # add this variables attributes
            for attrname in data[v].ncattrs():
                value=getattr(data[v], attrname)
                this_var.add_attribute(attrname,value)
            this_var.add_fid(fid)
            # find related coords
            dix=0
            for d in data[v].dimensions:
                cdix=np.where(this_dimnames==d)
                if len(cdix[0])==0:
                    print('cannot find dimname', dimname)
                    pdb.set_trace()
                cid=coords[this_cixes[cdix[0][0]]].cid
                this_var.add_cid(dix,cid)

                dix=dix+1

            # do we already have this variable
            matches=False
            for vix in range(nvars):
                matches=variables[vix].matches_variable(this_var)
                if matches:
                    break

            if matches==False:
                if verbose:
                    print('new variable', v, this_var.vid)
                variables.append(this_var)
                # we cannot add this to the database until the end when we have added all the fid cid pairs
                nvars=nvars+1
            else:
                variables[vix].copy_fid_cids_from_other(this_var)
                this_var=[]
                this_var=variables[vix]
                if verbose:
                    print('matching variable exists', v, this_var.vid)

             

    return ok, ncoords, nvars

#-----------------------------------------------------------------------------------
# Add to database the metadata from one hdf5 file
# Note this is not well tested yet
#-----------------------------------------------------------------------------------
import h5py
#-----------------------------------------------------------------------------------
# This descends the hierarchy of keys in an hdf5 file and creates coords and variables
# the coords will be added to teh Coords table, but the variables cannot be added to the
# database untill all files have been read and  the cids and fids have been set up
# inputs:
#    fid - is the id of the file we are reading
#    group is the data at this level of the hierarchy
#    coords - a list of the coords we have created so far
#    variables - a list of the variables we have created so far
#    coord_names - the names of keys that are coordinates rather than variables as given by the user
#    cur - a cursor to the database
#    update - allow updates to database - NOT YET IMPLEMENTED
#    verbose - control printing
#-----------------------------------------------------------------------------------
def read_keys(fid, group, coords, variables, coord_names, cur, update, verbose):
    keys=group.keys()
    print(keys)
    this_coord_cids=[]
    this_coord_names=[]
    next_cid=len(coords)
    next_vid=len(variables)

    # look for any coordinate data first
    key_names=np.asarray([key for key in keys])
    for name in coord_names:
        kix=np.where(np.asarray(key_names)==name)
        if len(kix[0])>0:
            key=key_names[kix[0][0]]
            this_group=group[key]
            print(this_group)
            if isinstance(this_group, h5py._hl.dataset.Dataset):
                atts=dict(this_group.attrs)
                if key in coord_names:
                    this_coord=Coord_metadata(next_cid, key, this_group)
                    print(key, 'is coord with cid', next_cid)
                    for attrname in atts:
                        value=atts.get(attrname)
                        if isinstance(value, bytes):
                            value=value.decode()
                        if attrname!='REFERENCE_LIST':
                            #print('coord attribute:',attrname, value)
                            this_coord.add_attribute(attrname, value)

                    # have we already got this coordinate?
                    matches=False
                    for c in range(len(coords)):
                        matches=coords[c].matches_coord(this_coord)
                        if matches:
                            break
                    if matches:
                        this_coord_cids.append(coords[c].cid)
                        print('coord matches', coords[c].cid)
                    else:
                        this_coord_cids.append(next_cid)
                        coords.append(this_coord)
                        next_cid=next_cid+1
                        this_coord.insert_into_database(cur,verbose)
                    this_coord_names.append(key)

    # now look at all the other keys
    for key in keys:
        if key not in coord_names:
            this_group=group[key]
            print(this_group)
            if isinstance(this_group, h5py._hl.dataset.Dataset):
                # this must be a variable
                ndims=len(this_group.shape)
                this_var=Variable_metadata(next_vid,this_group.name,ndims)
                this_var.add_fid(fid)
                print(key, 'is variable with shape', this_group.shape)
                atts=dict(this_group.attrs)
                dimension_found=np.zeros(ndims,int)
                for attrname in atts:
                    value=atts.get(attrname)
                    if isinstance(value, bytes):
                        value=value.decode()
                    if attrname=='DimensionNames':
                        print('dimensions:', value)
                        my_dimnames=value.split(',')
                        for d in range(ndims):
                            
                            if len(this_coord_names)>0 and dimension_found[d]==0:
                                ix=np.where(my_dimnames[d]==np.asarray(this_coord_names))
                                if len(ix[0])>0:
                                    this_cid=this_coord_cids[ix[0][0]]
                                    print('has coord',this_cid,'for dimension',d)
                                    this_var.add_cid(d,this_cid)
                                    dimension_found[d]=1
                                else:
                                    print(d, 'dimension not in coord_names')
                                    pdb.set_trace()

                    elif attrname!='DIMENSION_LIST' and attrname!='coordinates':
                        print('var attribute:',attrname, value)
                        this_var.add_attribute(attrname, value)

                # if we haven't found the dimension because there was no attribute called DimensionNames or coordinates
                # we will have to work out which coordinate goes with which dimension from shape
                dix=np.where(dimension_found==0)
                dimlen=np.asarray([coords[cid].nvals for cid in this_coord_cids])
                for d in dix[0]:
                    cix=np.where(dimlen==this_group.shape[d])
                    if len(cix[0])==1:
                        this_cid=this_coords_cids[cix[0][0]]
                        print('has coord',d,this_cid)
                        this_var.add_cid(d,this_cid)
                        dimension_found[d]=1
                    else:
                        print('cannot work out coordinate for dimension',d)
                        pdb.set_trace()
                dix=np.where(dimension_found==0)
                if len(dix[0])>0:
                    print('have not found all cordinates for variable',key)

                # do we already have this variable
                matches=False
                for vix in range(len(variables)):
                    matches=variables[vix].matches_variable(this_var)
                    if matches:
                        break

                if matches==False:
                    if verbose:
                        print('new variable', key, this_var.vid)
                    variables.append(this_var)
                    # we cannot add this to the database until the end when we have added all the fid cid pairs
                    next_vid=next_vid+1
                else:
                    variables[vix].copy_fid_cids_from_other(this_var)
                    this_var=[]
                    this_var=variables[vix]
                    if verbose:
                        print('matching variable exists', key, this_var.vid)

            elif isinstance(this_group,h5py._hl.group.Group):
                # this is a group
                read_keys(fid,this_group, coords, variables, coord_names, cur, update, verbose)
            else:
                print('what is this?')
                pdb.set_trace()

    print('end_for keys')


#-----------------------------------------------------------------------------------
# Adds to database the metadata from one hdf5 file.
# This will create the entry for the file in the Files table and calls read_keys to descend the hierarchy
# to read any coordinates and variables
# inputs:
#    dirpath - directory of file
#    filename - fiename of file
#    fid - is the id for the file we are reading
#    coords - a list of the coords we have created so far
#    variables - a list of the variables we have created so far
#    cur - a cursor to the database
#    update - allow updates to database - NOT YET IMPLEMENTED
#    verbose - control printing
# returns:
#    ok=True/False - indicates whether we could read the file
#-----------------------------------------------------------------------------------
def read_hdf5(dirpath, filename, fid, coords, variables,  cur, coord_names, update, verbose):

    ok=False
    filepath=get_filepath(dirpath,filename)

    try:
        if verbose:
            print('reading', filepath)
        group = h5py.File(filepath, 'r')
        ok=True

    except OSError as err:
        warnings.warn('cannot read file {filename}, error={err}'.format(filename=filename, err=err), UserWarning)
        print('cannot read file {filename}, error={err}'.format(filename=filename, err=err))
        return False

    this_file=File_metadata(fid, dirpath, filename)
    # get the global attributes
    atts = dict(group.attrs)
    for attrname in atts:
        value=atts.get(attrname).decode()
        this_file.add_attribute(attrname,value)

    this_file.insert_into_database(cur,verbose)
    read_keys(fid, group, coords, variables, coord_names, cur, update, verbose)
    return ok

#-----------------------------------------------------------------------------------
# code to build the database from the metadata of files of type ftype in basedir
# inputs:
#    basedir: the base directory to trawl
#    ftype: the type of files to read (currently netcdf (nc) and hdf5 (hdf5) is supported)
#           for nc we will only open files with .nc extension
#           
#    dbname: the name of the database (full path)
#    coord_names - a list of the names of keys in hdf5 files that are actually coordinates
#                  must be given for hdf5 but ignored for nc
#    update: if True, check all the file dates and if the file is not in the database
#               then add data from the file as new content, or if the date has changed
#               then update the records for this file - not yet implemented 
#
# -----------------------------------------------------------------------------------
def build_db(basedir, ftype, dbname, coord_names, update,verbose):

    # open the database dbname - this will create it if it does not exist
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    # check whether there are any tables
    res = cur.execute("SELECT name FROM sqlite_master")
    db_exists=False
    table_names=res.fetchall()
    if len(table_names)>0:
        print(table_names)
        db_exists=True
        if update==False:
            print(dbname, 'already exists')
            exit()
        else:
            res=cur.execute("""SELECT name FROM Variables""").fetchone()
            if len(res)>0:
                print('update not yet handled')
                exit()


    if db_exists==False:
        create_tables(cur)
    
    nfiles=0
    coords=[]
    variables=[]

    # extension may be in capitals or lower case and for hdf5 allow hdf5, HDF5, hdf and HDF
    if ftype=='hdf5':
        allowed_extension=[ftype, ftype.upper(), 'hdf', 'HDF']
    else:
        allowed_extension=[ftype, ftype.upper()]
    # now trawl through the directory structure from basedir
    if verbose:
        print('trawling directory', basedir, 'for', ftype)
    for dirpath, dirnames, filenames in os.walk(basedir):

        for filename in filenames:
            wsplit=filename.split('.')
            if len(wsplit)>1:
                if wsplit[-1] in allowed_extension:

                    if ftype=='nc':
                        ok=read_netcdf(dirpath, filename, nfiles, coords, variables, cur, update, verbose)
                    else:
                        ok=read_hdf5(dirpath, filename, nfiles, coords, variables, cur, coord_names, update, verbose)

                    if ok:
                        nfiles=nfiles+1

    for this_var in variables:
        this_var.insert_into_database(cur,verbose)
    # commit the changes
    con.commit()
    print(nfiles, 'Files', len(coords), 'Coords and', len(variables), 'Variables created')

# -----------------------------------------------------------------------------------
# main - read the arguments and call build_db
# -----------------------------------------------------------------------------------
def main():

    if len(sys.argv)<4:
        print('usage:', sys.argv[0], '<basedir> <filetype (nc/hdf5)> <database_name> <options eg -u to update, -v=verbose> <[coord1 coord2 coord3...]')
        exit()
    else:
        basedir=sys.argv[1]
        ftype=sys.argv[2] # this can be 'nc' for netcdf files and 'hdf5' for hdf5 files
        if ftype!='nc' and ftype!='hdf5':
            print('unknown filetype', ftype)
            exit()

        dbname=sys.argv[3]
        update=False
        verbose=False
        coord_names=[]
        for i in range(4,len(sys.argv)):
            if sys.argv[i]=='-u':
                update=True
            elif sys.argv[i]=='-v':
                verbose=True
            else:
                coord_names.append(sys.argv[i])
        if len(coord_names)==0 and ftype=='hdf5':
            print('coordinate names must be given')
            exit()

        if update==True:
            print('update not yet implemented')

    build_db(basedir, ftype, dbname, coord_names, update, verbose)



if __name__ == '__main__':
    main()
