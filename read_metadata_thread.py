
'''
    Code to extract metadata for netcdf or hdf5 files and store in sqlite3 database.
    This is used by the build_metadata_db.py to handle threading where a thread is
    set up for each file to be processed. It can also be used to just test reading of a single file.

    Looks in either the netcdf/hdf5 files (depending on ftype) to find what variables
    are there and stores the metadata for files, coordinates and variables in the database <database_name>.
    In the case of hdf5 files the names of the coordinates [coord1 coord2...] should be given because there is
    no guarantee that the metadata will be adequate to identify which keys are variables and which are
    coordinates.

    In future the -u option will be implemented so that we can check what is already in the database and
    make sure it is up to date


    Uses the threading library to make the building of the database multi-threaded

'''

import warnings
import threading
import numpy as np
import datetime as dt
import sqlite3
from netCDF4 import Dataset
from db_functions import *

#-----------------------------------------------------------------------------------
# Class to define the thread used to handle reading one file
# This creates a Files entry and several Coords entries into the database but does not
# insert the Variables as this is done at the end of reading all files
#-----------------------------------------------------------------------------------
class Read_metadata_thread(threading.Thread):

    # shared data between threads
    lock = threading.Lock()
    update=False  # if True, check all the file dates and if the file is not in the database
                  # then add data from the file as new content, or if the date has changed
                  # then update the records for this file - not yet implemented 
    verbose=False # control printing
    ftype=''      # the type of files to read (currently netcdf (nc) and hdf5 (hdf5) is supported)
                  # for nc we will only open files with .nc extension
    allowed_extension=[] # set up according to ftype in set_ftype()

    hdf5_coord_names=[]  # a list of the names of keys in hdf5 files that are actually coordinates
                         # must be given for hdf5 but ignored for nc

    con=None # shared connection to the database
    cur=None # shared cursor to the database
    # make sure python integers int32 and int64 are saved as INTEGER not BLOB
    sqlite3.register_adapter(np.int64, int) #lambda val: int(val))
    sqlite3.register_adapter(np.int32, int) #lambda val: int(val))
    nfiles=0
    coords=[]
    variables=[]
    bad_files=[]

    #------------------------------------------------------------------
    # setting up type of file we are looking for and allowed extensions
    #------------------------------------------------------------------
    def set_ftype(ftype):
        ok=False
        if ftype!='nc' and ftype!='hdf5':
            print('unknown filetype', ftype)
        else:
            Read_metadata_thread.ftype=ftype
            # extension may be in capitals or lower case and for hdf5 allow hdf5, HDF5, hdf and HDF
            if ftype=='hdf5':
                Read_metadata_thread.allowed_extension=[ftype, ftype.upper(), 'hdf', 'HDF']
            else:
                Read_metadata_thread.allowed_extension=[ftype, ftype.upper()]
            ok=True

        return ok

    #---------------------------------------------------------------------------------------
    # Function to create a file entry with the next available fid
    # This needs to acquire the lock while determining what the next available fid is
    # inputs:
    #    this_file - an instance of File_metadata to be added to the database (does not have valid fid)
    # returns:
    #    this_fid -  the fid of the newly created file entry
    #---------------------------------------------------------------------------------------
    def create_file_entry(self, this_file):
        # acquire lock to access nfiles shared data
        Read_metadata_thread.lock.acquire()
        # find next available fid
        this_fid=Read_metadata_thread.nfiles
        this_file.fid=this_fid
        Read_metadata_thread.nfiles+=1
        # store file entry in database
        this_file.insert_into_database(self.thread_name, Read_metadata_thread.cur,Read_metadata_thread.verbose)
        Read_metadata_thread.con.commit()
        Read_metadata_thread.lock.release()

        return this_fid

    #-----------------------------------------------------------------------------------------------------------------
    # Function to check whether this_coord already exists (doesn't need to match cid but should match everything else
    # if it doesn't exist then add it to coord list with next available cid and insert into database
    # inputs:
    #    this_coord - the new coordinate we need to match or create (does not have a valid cid)
    # returns:
    #    this_cid -  the cid of the newly created or matching coordinate
    #-----------------------------------------------------------------------------------------------------------------
    def create_or_find_matching_coord(self, this_coord):

        # acquire lock to access coords shared data
        Read_metadata_thread.lock.acquire()
        # do we already have this coordinate
        coord_matches=np.asarray([coord.matches_coord(this_coord) for coord in Read_metadata_thread.coords])
        matches=False
        ix=np.where(coord_matches)
        if len(ix[0])==1:
            this_coord=Read_metadata_thread.coords[ix[0][0]]
            this_cid=this_coord.cid
            matches=True
        if len(ix[0])>1:
            raise ValueError(self.thread_name+' Read_metadata_thread.create_or_find_matching_coord(): new coord matches more than one existing coord! '+this_coord.name) 

        if matches==False:
            # we don't have it so append it to coords list and store it in the database
            ncoords=len(Read_metadata_thread.coords)
            this_cid=ncoords
            this_coord.cid=this_cid
            Read_metadata_thread.coords.append(this_coord)
            this_coord.insert_into_database(self.thread_name, Read_metadata_thread.cur,Read_metadata_thread.verbose)
            Read_metadata_thread.con.commit()

        Read_metadata_thread.lock.release()
        if matches==True and Read_metadata_thread.verbose:
            print(self.thread_name, ' Read_metadata_thread.create_or_find_matching_coord(): matching coordinate exists', this_coord.name, this_coord.cid)

        return this_cid

    #--------------------------------------------------------------------------------------------------------
    # Function to check whether this_var already exists
    # if it doesn't exist then add it to variables list with next available vid
    # if it does then copy the fid and cid of this_var into the matching variable
    # Note we cannot add this_var to the database until the end when we have added all the fid cid pairs
    # inputs:
    #    this_var - the new variable we need to match or create (does not have a valid vid)
    #--------------------------------------------------------------------------------------------------------
    def create_or_find_matching_variable(self, this_var):
        # acquire lock to access variables shared data
        Read_metadata_thread.lock.acquire()
        var_matches=np.asarray([var.matches_variable(this_var,Read_metadata_thread.coords,Read_metadata_thread.verbose, self.thread_name) for var in Read_metadata_thread.variables])
        # do we already have this variable
        matches=False
        ix=np.where(var_matches)
        if len(ix[0])==1:
            Read_metadata_thread.variables[ix[0][0]].copy_fid_cids_from_other(this_var)
            if Read_metadata_thread.verbose:
                print(self.thread_name, ' Read_metadata_thread.create_or_find_matching_variable(): matching variable exists', this_var.name, this_var.vid)

            this_var=[]
            matches=True
        elif len(ix[0])>1:
            raise ValueError(self.thread_name+' Read_metadata_thread.create_or_find_matching_variable(): new var matches more than one existing var! '+this_var.name) 

        if matches==False:
            # add the new variable
            nvars=len(Read_metadata_thread.variables)
            this_var.vid=nvars
            Read_metadata_thread.variables.append(this_var)


        if Read_metadata_thread.verbose:
            if matches==False:
                print(self.thread_name, ' Read_metadata_thread.create_or_find_matching_variable(): New variable', this_var.name, this_var.vid, 'in files', this_var.fids, 'with cids', this_var.cids)


        Read_metadata_thread.lock.release()

    #-----------------------------------------------------------------------------------
    # initiation of thread to handle a file
    # 
    # inputs:
    #    this_dir - Directory object containing directory info of file
    #    filename - filename of file we are reading
    #-----------------------------------------------------------------------------------
    def __init__(self, this_dir, filename): 
        threading.Thread.__init__(self)
        self.this_dir=this_dir
        self.filename=filename
        self.thread_name=threading.current_thread().name+'_'+filename

    #-----------------------------------------------------------------------------------
    # Adds to database the metadata from one netcdf file.
    # This will create the entry for the file in the Files table and entries for any coords in the Coords table
    # Any variables will be held in the variables list but cannot be added to the database until we have read
    # all files and set up all the cids and fids.
    #
    # returns:
    #    ok=True/False - indicates whether we could read the file
    #-----------------------------------------------------------------------------------
    def read_netcdf(self):
        ok=False
        filepath=get_filepath(self.this_dir.dirpath, self.filename)

        try:
            if Read_metadata_thread.verbose:
                print(self.thread_name,' Read_metadata_thread.read_netcdf(): reading', filepath)
            data=Dataset(filepath, "r", format="NETCDF4")
            ok=True

        except OSError as err:

            warnings.warn(self.thread_name+' Read_metadata_thread.read_netcdf(): Cannot read file {filename}, error={err}'.format(filename=filepath, err=err), UserWarning)
            Read_metadata_thread.lock.acquire()
            Read_metadata_thread.bad_files.append(filepath)
            Read_metadata_thread.lock.release()
            return ok
            
        this_file=File_metadata(UNKNOWN_ID, self.this_dir.did, self.this_dir.dirpath, self.filename)
        # get the global attributes
        this_file.global_attributes=[Attribute(attrname,getattr(data, attrname)) for attrname in data.ncattrs()]
        this_fid=self.create_file_entry(this_file)

        # get the coords from this file - remember the cids and dimnames to match with the variables
        this_cids=[]
        this_dimnames=[]
        # a coordinate is a dimension of a variable but it is usually also stored in netcdf as a
        # variable too because it has values and attributes
        vkeys=data.variables.keys()
        for d in data.dimensions:
            if d in vkeys:
                # read the information about this coordinate by creating a coordinate instance
                this_coord=Coord_metadata(UNKNOWN_ID, d, data[d][:], self.thread_name)
                # need to add one attribute at a time so we can check for units and calendar attributes
                for attrname in data[d].ncattrs():
                    value=getattr(data[d], attrname)
                    this_coord.add_attribute(attrname,value)
            else:
                # there is no information for this coordinate but we must still create a coordinate
                this_coord=Coord_metadata(UNKNOWN_ID, d, [], self.thread_name)
            this_cid=self.create_or_find_matching_coord(this_coord)
            this_cids.append(this_cid)
            this_dimnames.append(d)
        this_dimnames=np.asarray(this_dimnames)


        # get the variable names that are not in dimensions
        for v in data.variables:
            is_dim=False
            if v in data.dimensions:
                is_dim=True
            if is_dim==False:
                # this is a proper variable so create a variable instance
                ndims=len(data[v].dimensions)
                this_var=Variable_metadata(UNKNOWN_ID,v,ndims)
                # add this variables attributes
                this_var.attributes=[Attribute(attrname,getattr(data[v], attrname)) for attrname in data[v].ncattrs()]
                # find related coords
                if ndims>0:
                    cdixes=[np.where(this_dimnames==d)[0] for d in data[v].dimensions]
                    found=np.asarray([len(cdixes)>0 for d in data[v].dimensions])
                    ix=np.where(found==False)    
                    if len(ix[0])>0:
                        raise ValueError(self.thread_name+': Read_metadata_thread.read_netcdf(): cannot find dimnames for dims {}'.format(ix[0]))
                    else:
                        cids=[this_cids[c[0]] for c in cdixes]
                else:
                    cids=[]
                if Read_metadata_thread.verbose:
                    print(self.thread_name, ' Read_metadata_thread.read_netcdf(): creating new variable to check if it exists', this_var.name, 'fid=',this_fid, 'cids=', cids, len(Read_metadata_thread.variables), 'existing vars')
                this_var.add_cids_for_fid(this_fid, cids)
                self.create_or_find_matching_variable(this_var)             

        return ok

    #-----------------------------------------------------------------------------------
    # Add to database the metadata from one hdf5 file
    # Note this is not well tested yet
    #-----------------------------------------------------------------------------------
    import h5py
    # get the attributes from the data for this variable
    # attributes may tells us about the dimension names of the variable which we can link to coordinates
    def build_attribute_list(self, varname, atts, ndims, this_coord_names, this_coord_cids):
    
        dimension_found=np.zeros(ndims,int)
        var_cids=np.zeros(ndims,int)-1
        attributes_list=[]
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
                                var_cids[d]=this_coord_cids[ix[0][0]]
                                print(self.thread_name+' Read_metadata_thread.build_attribute_list():',varname,'has coord',this_cid,'for dimension',d)
                                dimension_found[d]=1
                            else:
                                raise ValueError(self.thread_name+' Read_metadata_thread.build_attribute_list(): dimension {} not in coord_names for var {}'.format(d, varname))

                        elif attrname!='DIMENSION_LIST' and attrname!='coordinates':
                            print('var attribute:',attrname, value)
                            attribute_list.append(Attribute(attrname,value))
                            
        return attributes_list, dimension_found
        
    #-----------------------------------------------------------------------------------
    # This descends the hierarchy of keys in an hdf5 file and creates coords and variables
    # the coords will be added to the Coords table, but the variables cannot be added to the
    # database untill all files have been read and  the cids and fids have been set up
    # inputs:
    #    fid - is the id of the file we are reading
    #    group is the data at this level of the hierarchy
    #-----------------------------------------------------------------------------------
    def read_keys(self, fid, group):
        keys=group.keys()
        print(keys)
        this_coord_cids=[]
        this_coord_names=[]

        # look for any coordinate data first
        key_names=np.asarray([key for key in keys])
        for name in Read_metadata_thread.hdf5_coord_names:
            kix=np.where(np.asarray(key_names)==name)
            if len(kix[0])>0:
                key=key_names[kix[0][0]]
                this_group=group[key]
                print(this_group)
                if isinstance(this_group, h5py._hl.dataset.Dataset):
                    atts=dict(this_group.attrs)
                    if key in coord_names:
                        this_coord=Coord_metadata(UNKNOWN_ID, key, this_group)
                        for attrname in atts:
                            value=atts.get(attrname)
                            if isinstance(value, bytes):
                                value=value.decode()
                            if attrname!='REFERENCE_LIST':
                                #print('coord attribute:',attrname, value)
                                this_coord.add_attribute(attrname, value)

                        this_cid=self.create_or_find_matching_coord(this_coord)
                        this_coord_cids.append(this_cid)
                        this_coord_names.append(key)

        # now look at all the other keys
        for key in keys:
            if key not in coord_names:
                this_group=group[key]
                print(this_group)
                if isinstance(this_group, h5py._hl.dataset.Dataset):
                    # this must be a variable
                    ndims=len(this_group.shape)
                    this_var=Variable_metadata(UNKNOWN_ID,this_group.name,ndims)
                    print(key, 'is variable with shape', this_group.shape)
                    atts=dict(this_group.attrs)
                    attributes_list, dimension_found=build_attribute_list(this_var.name, atts, ndims, this_coord_names, this_coord_cids)
                    this_var.attributes=attributes_list

                    # if we haven't found the dimension because there was no attribute called DimensionNames or coordinates
                    # we will have to work out which coordinate goes with which dimension from shape
                    dix=np.where(dimension_found==0)
                    dimlen=np.asarray([Read_metadata_thread.coords[cid].nvals for cid in this_coord_cids])
                    for d in dix[0]:
                        cix=np.where(dimlen==this_group.shape[d])
                        if len(cix[0])==1:
                            var_cids[d]=this_coords_cids[cix[0][0]]
                            print('has coord',d,var_cids[d])
                            dimension_found[d]=1
                        else:
                            raise ValueError(self.thread_name+' Read_metadata_thread.read_keys(): cannot work out coordinate for dimension {}'.format(d))

                    dix=np.where(dimension_found==0)
                    if len(dix[0])>0:
                        raise ValueError(self.thread_name+' Read_metadata_thread.read_keys(): have not found all coordinates for variable '+this_var.name)
                    else:
                        this_var.add_cids_for_fid(fid, var_cids)

                    self.create_or_find_matching_variable(this_var)             

                elif isinstance(this_group,h5py._hl.group.Group):
                    # this is a group
                    read_keys(fid,this_group)
                else:
                    raise ValueError(self.thread_name+' Read_metadata_thread.read_keys(): Unknown type of this_group {}'.format(type(this_group)))



    #-----------------------------------------------------------------------------------
    # Adds to database the metadata from one hdf5 file.
    # This will create the entry for the file in the Files table and calls read_keys to descend the hierarchy
    # to read any coordinates and variables
    # returns:
    #    ok=True/False - indicates whether we could read the file
    #-----------------------------------------------------------------------------------
    def read_hdf5(self):

        ok=False
        filepath=get_filepath(self.this_dir.dirpath, self.filename)

        try:
            if Read_metadata_thread.verbose:
                print(self.thread_name,' Read_metadata_thread.read_hdf5(): reading', filepath)
            group = h5py.File(filepath, 'r')
            ok=True

        except OSError as err:
            warnings.warn(self.thread_name+' Read_metadata_thread.read_hdf5(): Cannot read file {filename}, error={err}'.format(filename=filepath, err=err), UserWarning)
            Read_metadata_thread.lock.acquire()
            bad_files.append(filepath)
            Read_metadata_thread.lock.release()
            return False

        this_file=File_metadata(UNKNOWN_ID, this_dir.did, this_dir.dirpath, self.filename)
        # get the global attributes
        atts = dict(group.attrs)
        for attrname in atts:
            value=atts.get(attrname).decode()
            this_file.add_attribute(attrname,value)
        this_fid=self.create_file_entry(this_file)

        self.read_keys(this_fid, group)

        return ok


    #-----------------------------------------------------------------------------------
    # function called on starting thread
    #-----------------------------------------------------------------------------------
    def run(self):

        self.thread_name=threading.current_thread().name

        if Read_metadata_thread.ftype=='nc':
            ok=self.read_netcdf()
        else:
            ok=self.read_hdf5()
        return ok
