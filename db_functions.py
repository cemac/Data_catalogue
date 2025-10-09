'''
    Code for all the database related functions:

    Defines classes that hold data related to database tables and handle reading/writing the relevant data
    from/to the database.

    These functions are called by both build_metadata_db.py and metaview.py
    When building the database we have no idea how many files, coordinates or variables there
    will be so we have to use append to build lists. This is not ideal when there could be 1000's 
    of files and coordinates. When reading from the database we can check how many rows so can 
    preallocate space.

'''

import pdb
import sqlite3
from netCDF4 import num2date, date2num
import numpy as np
import datetime as dt
import os
import string

FILE_SPECIFIC_VAL='File specific'  # used to set the value of an attribute that we don't really care about
                                   # and is different for different variables in different files
UNKNOWN_ID=-1

#--------------------------------------------------------------------------------------------
# create all the tables we need to store metadata
#--------------------------------------------------------------------------------------------
def create_tables(cur,verbose=False):
    if verbose:
        print('Creating tables')
    cur.execute("CREATE TABLE Directories(did INTEGER PRIMARY KEY, dirpath TEXT)")
    cur.execute("CREATE TABLE Files(fid INTEGER PRIMARY KEY, did INTEGER, filename TEXT, symlink TEXT, created REAL, modified REAL)")
    cur.execute("CREATE TABLE Global_Attributes(fid INTEGER, name TEXT, value)")
    cur.execute("CREATE TABLE Coords(cid INTEGER PRIMARY KEY, name TEXT, nvals INTEGER, min_val REAL, max_val REAL, delta REAL)")
    cur.execute("CREATE TABLE Discrete_Coord_Values(cid INTEGER, value REAL)")
    cur.execute("CREATE TABLE Coord_Attributes(cid INTEGER, name TEXT, value)")
    cur.execute("CREATE TABLE Variables(vid INTEGER PRIMARY KEY, name TEXT, ndims INTEGER)")
    cur.execute("CREATE TABLE Coords_Fids_Of_Variables(vid INTEGER, cid INTEGER, fid INTEGER, dimix INTEGER)")
    cur.execute("CREATE TABLE Var_Attributes(vid INTEGER, name TEXT, value)")

#-----------------------------------------------------------------
# function to combine a directory path and filename to give a filepath
#----------------------------------------------------------------
def get_filepath(dirpath, filename):
    return dirpath+'/'+filename


#--------------------------------------------------------------------------------------------
# define classes to hold data read from database and the code to read them from the database
#--------------------------------------------------------------------------------------------

#------------------------------------------
# class to hold data for one directory
#------------------------------------------
class Directory:
    def __init__(self,*args):
        # if one arg this is row for initiation from database otherwise did and dirname are
        # given when creating database 
        if len(args) == 1: 
            self.init_from_database(*args)
        else:
            self.init_from_data(*args)

    #---------------------------------------------------------------------------------------
    # initiation from database where row is value returned from a fetchall() after a
    # SELECT statement for all values from the Directories table
    #---------------------------------------------------------------------------------------
    def init_from_database(self, row):
        self.did=row[0]
        self.dirpath=row[1]

    #---------------------------------------------------------------------------------------
    # initiation when creating database.
    #---------------------------------------------------------------------------------------
    def init_from_data(self,did,dirpath):
        self.did=did
        self.dirpath=dirpath

    #---------------------------------------------------------------------------------------
    # inserts directory data into Directories table
    #---------------------------------------------------------------------------------------
    def insert_into_database(self, thread_name, cur,verbose=False):
        # create an entry in Directories table for this directory
        if verbose:
            print(thread_name, ' Directory.insert_into_database(): Creating Directory entry', self.did, self.dirpath)
        cur.execute("""INSERT INTO Directories (did, dirpath) VALUES(?,?)""",(self.did, self.dirpath))


#---------------------------------------------------------------------------------------
# Attributes are used for file attributes, coordinate attributes and variable attributes
#    name is always a str
#    value may be a float or a str
#---------------------------------------------------------------------------------------
class Attribute:
    def __init__(self,attrname,value):
        self.name=attrname
        # sqlite3 doesn't handle integers well - it creates blobs depending on size of integer so best
        # to save as float or string
        if isinstance(value, np.ndarray):
            # we cannot store an array as a database value so we will have to convert this to a single string
            words=[str(v) for v in value]
            value='['+', '.join(words)+']'
        elif isinstance(value, str)==False:
            value=float(value)
        self.value=value
        
    #---------------------------------------------------------------------------------------
    # combine the attribute name and value into a string
    #---------------------------------------------------------------------------------------
    def get_attr_str(self):
        this_attr_str=self.name+' : '
        if isinstance(self.value, str):
            this_attr_str=this_attr_str+self.value
        else:
            if self.value<0.01:
                this_attr_str=this_attr_str+'{v:.2g}'.format(v=self.value)
            else:
                this_attr_str=this_attr_str+'{v:.2f}'.format(v=self.value)

        return this_attr_str+'\n'
        
#--------------------------------------------------
# File_metadata holds the data for one file
#--------------------------------------------------
class File_metadata:
    
    def __init__(self,*args):
        # if 2 args these are row and cur for initiation from database otherwise this is
        # initiation when creating database 
        if len(args) == 2: 
            self.init_from_database(*args)
        else:
            self.init_from_data(*args)

    #---------------------------------------------------------------------------------------
    # get the global attributes from the database
    #---------------------------------------------------------------------------------------
    def read_global_attributes(self, cur):
        # select all data from Global_Attributes table
        cur.execute("""SELECT name, value FROM Global_Attributes WHERE fid=?""",(self.fid,))
        self.global_attributes=[Attribute(row[0],row[1]) for row in cur.fetchall()]

            
    #---------------------------------------------------------------------------------------
    # initiation from database where row is value returned from a fetchall() after a
    # SELECT statement for all values from the Files table
    # cur is the cursor to the database so we can get matching global attributes
    # we wont read the global attributes at this point because it makes initiation slow
    # use read_global_attributes() to read them only when you need them 
    #---------------------------------------------------------------------------------------
    def init_from_database(self,row, cur):
        self.fid=row[0]
        self.did=row[1]
        self.filename=row[2]
        self.symlink=row[3]
        self.created=row[4]
        self.modified=row[5]

    #---------------------------------------------------------------------------------------
    # initiation when creating database. This sets up all but the global attributes
    # global attributes can be added later. 
    #---------------------------------------------------------------------------------------
    def init_from_data(self, fid, did, dirpath, filename):
        self.fid=fid
        self.did=did
        self.filename=filename
        self.symlink=''
        # check whether this file is a symbolink link and what its creation and modification dates are
        my_filepath=get_filepath(dirpath, filename)
        if os.path.islink(my_filepath):
            self.symlink=os.readlink(my_filepath)
            # get dates as epoch time for actual file in symbolic link
            self.created=os.path.getctime(self.symlink)
            self.modified=os.path.getmtime(self.symlink)
        else:
            self.created=os.path.getctime(my_filepath)
            self.modified=os.path.getmtime(my_filepath)
        self.global_attributes=[]

    #---------------------------------------------------------------------------------------
    # called after init_from_data to add attributes
    # name should be a str
    # value can be a str or int or float
    #---------------------------------------------------------------------------------------
    #def add_attribute(self, i, name, value):
    #    self.global_attributes[i]=Attribute(name,value)


    #---------------------------------------------------------------------------------------
    # inserts file data into Files table and any global attributes into Global_Attributes table
    #---------------------------------------------------------------------------------------
    def insert_into_database(self, thread_name, cur,verbose=False):
        # create an entry in Files table for this file
        if verbose:
            print(thread_name, ' File_metadata.insert_into_database(): Creating File entry', self.fid, self.filename)
        cur.execute("""INSERT INTO Files (fid, did, filename, symlink, created, modified) VALUES(?,?,?,?,?,?)""",
                   (self.fid, self.did, self.filename, self.symlink, self.created, self.modified))
        for att in self.global_attributes:
            cur.execute("""INSERT INTO Global_Attributes(fid, name, value) VALUES (?,?,?)""",
                        (self.fid, att.name, att.value))


    #--------------------------------------------------------------------------------------------------
    # combine the file global attributes into a string one on each line
    # we may need to read them if not already read
    # inputs:
    #   cur 
    # returns:
    #   the combined string
    #--------------------------------------------------------------------------------------------------
    def get_file_attr_str(self, cur):
        if hasattr(self,'global_attributes')==False:
            print('reading global attributes for file')
            self.read_global_attributes(cur)
        attr_str=''
        if len(self.global_attributes)==0:
            attr_str='No global attributes\n'
        max_line_len=len(attr_str)
        for attr in self.global_attributes:
            this_attr_str=attr.get_attr_str()
            if len(this_attr_str)>max_line_len:
                 max_line_len=len(this_attr_str)
            attr_str=attr_str+this_attr_str
        return attr_str, max_line_len

    #--------------------------------------------------------------------------------------------------
    # combine the file information into a string
    # this does not include the global attributes
    # inputs:
    #   the dirpath that this file is in
    # returns:
    #   the combined string
    #--------------------------------------------------------------------------------------------------
    def get_file_info_str(self,dirpath):
        created_date =  dt.datetime.fromtimestamp(self.created)
        modified_date = dt.datetime.fromtimestamp(self.modified)
        file_text=get_filepath(dirpath, self.filename)
        sym_text=', symlink='+self.symlink
        create_text=', created='+created_date.strftime('%Y-%m-%d %H:%M')
        mod_text=', modified='+modified_date.strftime('%Y-%m-%d %H:%M')
        return file_text+create_text+mod_text+sym_text+'\n'

    #---------------------------------------------------------------------------------------
    # function to print info about this file including the attributes
    #---------------------------------------------------------------------------------------
    def print(self):
        print(self.fid, self.did, self.filename, self.symlink, self.created, self.modified)
        for att in self.global_attributes:
            print('\t', att.name, att.value)

#--------------------------------------------------
# Files_metadata holds the data for all files
#--------------------------------------------------
class Files_metadata:

    #---------------------------------------------------------------------------------------
    # initiation 
    #---------------------------------------------------------------------------------------
    def __init__(self):
        self.all_files_metadata=[]

    #---------------------------------------------------------------------------------------
    # read files from database where cur is a cursor for the database and did is the directory id
    # if did=-1 then we get all files otherwise select those with matching did
    # if filename_exp=='' get all files, otherwise select those that match
    #---------------------------------------------------------------------------------------
    def read_from_database(self,cur,did=-1, filename_exp=''):
        if self.get_nfiles()>0:
            self.clear()
        # select all data from Files table
        if did==-1:
            cur.execute("""SELECT fid, did, filename, symlink, created, modified FROM Files""")
        else:
            cur.execute("""SELECT fid, did, filename, symlink, created, modified FROM Files WHERE did=?""", (did,))
        for row in cur.fetchall():
            this_file=File_metadata(row, cur)
            if filename_exp!='':
                match=this_file.filename.find(filename_exp)
            else:
                match=0
            if match>=0:
                self.all_files_metadata.append(this_file)
            
    #---------------------------------------------------------------------------------------
    # returns number of files stored
    #---------------------------------------------------------------------------------------
    def get_nfiles(self):
        return len(self.all_files_metadata)


    #---------------------------------------------------------------------------------------
    # clear the list of files
    #---------------------------------------------------------------------------------------
    def clear(self):
        self.all_files_metadata.clear()

    #---------------------------------------------------------------------------------------
    # returns a list of the fids of the files that have a directory id that equals the did requested
    #---------------------------------------------------------------------------------------
    def get_fids_for_matching_did(self,did):
        matches=[this_file.did==did for this_file in self.all_files_metadata]
        ix=np.where(matches)
        fids=[self.all_files_metadata[f].fid for f in ix[0]]
        return fids

    #---------------------------------------------------------------------------------------
    # returns a list of the fids of all the files in this list
    #---------------------------------------------------------------------------------------
    def get_fids(self):
        fids=[f.fid for f in self.all_files_metadata]
        return fids
    
    #---------------------------------------------------------------------------------------
    # returns the file_metadata for the file with an fid that matches that given, (None if not found)
    # if all the files have been read from the database then the fid is the same as the index into the array
    #---------------------------------------------------------------------------------------
    def get_matching_fid(self,fid):
        this_file=None
        if fid<self.get_nfiles() and self.all_files_metadata[fid].fid==fid:
            this_file=self.all_files_metadata[fid]
        else:
            matches=[this_file.fid==fid for this_file in self.all_files_metadata]
            ix=np.where(matches)
            if len(ix[0])==1:
                this_file=self.all_files_metadata[ix[0][0]]
        return this_file
    
    def print(self):
        for this_file in self.all_files_metadata:
           this_file.print()

#--------------------------------------------------
# Coord_metadata holds the metadata for a coordinate
#--------------------------------------------------
# Coord_filter holds the data from a filter that is used to select variables by comparing
# min and max vals with those of the Coordinate
class Coord_filter:
    def __init__(self,name):
        self.name=name
        self.is_time=False
        # once we have checked whether a coordinate with a name like name exists in the database is_valid could be set to False
        # if there was no matching coordinate and is_valid=False then the filter wont be displayed
        self.is_valid=True 
        self.min_val=None
        self.max_val=None
        self.min_widget=None # reference to the widget on the GUI for the min val
        self.max_widget=None # reference to the widget on the GUI for the max_val

    def get(self):
        min_val=self.min_widget.get()
        if min_val=='':
            self.min_val=None
        else:
            if self.is_time:
                self.min_val=dt.datetime.strptime(min_val, '%Y-%m-%d').timestamp() # keep as epoch time
            else:
                self.min_val=float(min_val)

        max_val=self.max_widget.get()
        if max_val=='':
            self.max_val=None
        else:
            if self.is_time:
                self.max_val=dt.datetime.strptime(max_val, '%Y-%m-%d').timestamp() # keep as epoch time
            else:
                self.max_val=float(max_val)

# Coord_metadata holds the data that is in the Coord table
# Coordinates that hold time information will store the time as the netcdf file did with accompanying units and calendar
# but when comparing times we will convert to epoch times
class Coord_metadata:

    def __init__(self,*args):
        # there are 2 possible ways to initialise a Coord
        # init_from_data is initation when reading metadata from file and takes cid, name, coord_values and thread_name
        # init_from_database - is initiation from the database and takes row and cur from Coord table
        if len(args)==2: 
            self.init_from_database(*args)
        else:
            self.init_from_data(*args)

    #---------------------------------------------------------------------------------------
    # get the attributes from the database
    #---------------------------------------------------------------------------------------
    def read_attributes(self, cur):
        # select all data from Coord_Attributes table for this cid
        cur.execute("""SELECT name,value,cid FROM Coord_Attributes WHERE cid=?""", (self.cid,))
        [self.add_attribute(attr[0], attr[1]) for attr in cur.fetchall()]


    #-------------------------------------------------------------------------------------------------------
    # initiate from database where row is a single row from a SELECT from the Coord table with all columns
    # finds the matching attributes for this coordinate from the Coord_Attributes table in the database 
    # if the coordinate values were not evenly spaced eg pressure levels, the actual values can be
    # found in the Discrete_Coord_Values table 
    # inputs:
    #    row is a single row from the SELECT from Coord table
    #    cur is a cursor on the database
    #-------------------------------------------------------------------------------------------------------
    def init_from_database(self,row, cur):
        self.cid=row[0]
        self.name=row[1]
        self.nvals=row[2]
        self.min_val=row[3]
        self.max_val=row[4]
        # if the data read from netcdf was Nan it will be stored in the database as None but set it to np.nan here
        if self.min_val==None:
            self.min_val=np.nan
        if self.max_val==None:
            self.max_val=np.nan
            
        self.delta=row[5]
        self.values=[]   # used to hold discrete values if there are any
        self.attributes=[]
        self.units_attrix=-1
        self.calendar_attrix=-1
        self.read_attributes(cur)
        
        if self.delta==0:
            cur.execute("""SELECT value FROM Discrete_Coord_Values WHERE cid=?""", (self.cid,))
            val_rows=cur.fetchall()
            if len(val_rows)>0:
                self.values=np.asarray([val[0] for val in val_rows])

    #--------------------------------------------------
    # initiate from reading datafile (coord_values can be an empty list)
    #--------------------------------------------------
    def init_from_data(self,cid, name, coord_values, thread_name):
        self.cid=cid
        self.name=name
        self.delta=0
        self.nvals=len(coord_values)
        self.values=[]

        # sqlite3 doesn't handle integers well - it creates blobs depending on size of integer so best
        # to save data as float
        if self.nvals>0:
            assert(isinstance(coord_values[0], str)==False) # we don't handle str type coord values
            if hasattr(coord_values, 'mask'):
                # there are occasions when the coord_values are integer but have a mask
                # in that case we need to convert coord_values to float so we can set the masked values to nan
                was_int=False
                if isinstance(coord_values.data[0], np.int64) or isinstance(coord_values.data[0], np.int32) or isinstance(coord_values.data[0], int): # if the values were integer we have to cast to float so we can add nan
                    was_int=True
                    coord_type=type(coord_values.data[0])
                    coord_values2=np.zeros(len(coord_values.data))
                    coord_values2[:]=coord_values.data
                else:
                    coord_values2=coord_values.data
                # sometimes mask is not an array when data is
                if isinstance(coord_values.mask, np.ndarray):
                    ix=np.where(coord_values.mask==True)
                    if len(ix[0])>0:
                        coord_values2[ix]=np.nan
                        extra=''
                        if was_int:
                            extra=f'was int type {coord_type}'
                        print(f'{thread_name}: Coord_metadata.init_from_data(): cid {self.cid} {self.name} has masked coord values', extra)
                             
                else:
                    # check the mask is false otherwise all data is nan
                    assert(coord_values.mask==False)
                coord_values=coord_values2

            self.min_val=float(np.nanmin(coord_values))
            self.max_val=float(np.nanmax(coord_values))

            if self.nvals>1:
                deltas=abs(coord_values[1:]-coord_values[:-1])
                # in case there are some nan values just use the non-nan deltas
                ix=np.where(np.isfinite(deltas))
                deltas=deltas[ix[0]]
                delta_deltas=deltas[1:]-deltas[:-1]
                ix=np.where(abs(delta_deltas)>0.0001)
                if len(ix[0])==0:
                    self.delta=float(np.mean(deltas))
                else:
                    self.values=[float(v) for v in coord_values]
        else:
            self.min_val=np.nan
            self.max_val=np.nan

        self.attributes=[] # attributes are added by calling add_attribute()
        self.units_attrix=-1
        self.calendar_attrix=-1

    #----------------------------------------------------------------------------------------
    # add an attribute when creating from data file
    # check whether attribute is units or calendar so we can check if this is a time coord
    #----------------------------------------------------------------------------------------
    def add_attribute(self, name, value):
        self.attributes.append(Attribute(name,value))
        if name=='units':
            self.units_attrix=len(self.attributes)-1
        elif name=='calendar':
            self.calendar_attrix=len(self.attributes)-1

    #----------------------------------------------------------------------------------------
    # this coordinate is a time coordinate if it has units that start "months/days/hours/seconds since"
    # it may also have calendar attributes but if not, assume gregorian
    #----------------------------------------------------------------------------------------
    def is_time(self):
        is_time=False
        calendar=None
        if self.units_attrix>=0:
            units=self.attributes[self.units_attrix].value
            suspected_time=False
            if units.startswith('months') or units.startswith('days') or units.startswith('hours') or units.startswith('seconds') or self.calendar_attrix>=0:
                suspected_time=True
                # this is how I was testing it but it ought to have since in he units too
            if units.startswith('months since') or units.startswith('days since') or units.startswith('hours since') or units.startswith('seconds since') or self.calendar_attrix>=0:
                is_time=True
                if self.calendar_attrix>=0:
                    calendar=self.attributes[self.calendar_attrix].value
                else:
                    calendar='gregorian'
            elif suspected_time:
                print(self.cid, self.name, 'suspected_time but not good units', units)
        return is_time, calendar

    #----------------------------------------------------------------------------------------
    # check whether this coord has same metadata as given coord
    # cid doesn't matter but other values should match
    # also both should have matching attribute names and values
    # other is another instance of coord
    #---------------------------------------------------------------------------------------
    def matches_coord(self, other):
        matches=False
        if self.name==other.name and self.nvals==other.nvals:
            # handle min_val or max_val being NaN
            min_val_match= ((np.isnan(self.min_val) and np.isnan(other.min_val)) or abs(self.min_val-other.min_val)<1e-6) 
            max_val_match= ((np.isnan(self.max_val) and np.isnan(other.max_val)) or abs(self.max_val-other.max_val)<1e-6)
            values_match=False
            if len(self.values)>0 and len(other.values)>0:
                if np.all(abs(np.asarray(self.values)-np.asarray(other.values))<1e-6):
                    values_match=True
            else:
                if abs(self.delta-other.delta)<1e-6:
                    values_match=True
            if min_val_match and max_val_match and values_match:
                matches=True

        # now check the attributes - they must both match but don't need to be in the same order
        if matches:
            nattr=len(self.attributes)
            nother_attr=len(other.attributes)
            if nattr!=nother_attr:
                matches=False
            else:
                for i in range(nattr):
                    name_matches=np.asarray([attr.name==self.attributes[i].name for attr in other.attributes])
                    ix=np.where(name_matches)
                    if len(ix[0])==0:
                        matches=False
                        break
                    else:
                        # name matches but does value?
                        j=ix[0][0]
                        if self.attributes[i].value!=other.attributes[j].value:
                            matches=False
                            break

        return matches
        
    #----------------------------------------------------------------------------------------
    # Check whether this coord has same metadata as given coord but don't worry about values
    # Both should have matching name and attribute names and values
    # This is used to check the coordinates of the variables to see if they are the same type
    # for example a time coordinate that has different values to a different time coordinate matches type
    # as long as the calendar is the same and attributes units and time_origin can be different
    # other is another instance of coord
    #---------------------------------------------------------------------------------------
    def matches_coord_type(self, other, verbose, thread_name):
        matches=False
        if self.cid==other.cid:
            return True # other is the same thing
        if self.name==other.name:
            matches=True
            # now check the attributes - they must both match but don't need to be in the same order
            nattr=len(self.attributes)
            nother_attr=len(other.attributes)
            if nattr!=nother_attr:
                matches=False
                if verbose:
                    print(thread_name+' Coord_metadata.matches_coord_type(): mismatching number of attributes of coords', self.cid, other.cid)
            else:
                is_time, calendar=self.is_time()
                for i in range(nattr):
                    name_matches=np.asarray([attr.name==self.attributes[i].name for attr in other.attributes])
                    ix=np.where(name_matches)
                    if len(ix[0])==0:
                        matches=False
                        if verbose:
                            print(thread_name+' Coord_metadata.matches_coord_type(): attribute of coord missing in other', self.cid, other.cid, self.name, 'attribute',self.attributes[i].name)
                        break
                    else:
                        # allow times to have different units but they must have the same calendar
                        if is_time==False or (self.attributes[i].name!='units' and self.attributes[i].name!='time_origin'):
                            # name matches but does value?
                            j=ix[0][0]
                            if self.attributes[i].value!=other.attributes[j].value:
                                matches=False
                                if verbose:
                                    print(thread_name+' Coord_metadata.matches_coord_type(): mismatching attribute value of coords', self.cid, other.cid, self.name, 'attribute',self.attributes[i].name)
                                break
        elif verbose:
            print(thread_name+' Coord_metadata.matches_coord_type(): mismatching name of coords', self.name, other.name)

        return matches

    #----------------------------------------------------------------------------------------
    # code to insert coord into Coords table, any values into Discrete_Coord_Values table
    # and any attributes into Coords_Attributes table
    # cur is the cursor for the database
    #----------------------------------------------------------------------------------------
    def insert_into_database(self, thread_name, cur,verbose=False):
        # create Coord entry
        if verbose:
            if self.delta>0:
                extra=f'every {self.delta}'
            else:
                if self.nvals>0:
                    extra='with discrete values'
                else:
                    extra='no values'
            print(thread_name, ' Coord_metadata.insert_into_database(): Creating Coord entry', self.cid, self.name, 'nvals=',self.nvals, 'min=',self.min_val, 'max=',self.max_val, extra)
        cur.execute("""INSERT INTO Coords (cid, name, nvals, min_val, max_val, delta) VALUES (?,?,?,?,?,?)""",
                    (self.cid, self.name, self.nvals, self.min_val, self.max_val, self.delta))
        if len(self.values)>0:
            for i in range(self.nvals):
                cur.execute("""INSERT INTO Discrete_Coord_Values (cid, value) VALUES (?,?)""", (self.cid, self.values[i]) )
        if len(self.attributes)>0:
            for att in self.attributes:
                if verbose:
                    print(thread_name, ' Coord_metadata.insert_into_database(): creating coord attribute for cid',self.cid, att.name,att.value) 
                cur.execute("""INSERT INTO Coord_Attributes (cid, name, value) VALUES (?,?,?)""", (self.cid, att.name, att.value))
                
    #----------------------------------------------------------------------------------------
    # convert value to an epoch time - should only be called if we know this is a datetime coordinate
    #----------------------------------------------------------------------------------------
    def get_epoch_time(self, value):
        if np.isnan(value):
            epoch=value
        else:
            if self.calendar_attrix>=0:
                calendar=self.attributes[self.calendar_attrix].value
            else:
                calendar='gregorian'
            this_date=num2date(value,units=self.attributes[self.units_attrix].value,calendar=calendar)
            epoch_delta=this_date-num2date(0, 'hours since 1970-01-01', calendar=calendar)
            epoch=epoch_delta.days*24*3600+epoch_delta.seconds

        return epoch
    
    #----------------------------------------------------------------------------------------
    # this function returns the min_val, max_val and delta value of this coordinate
    # If the coordinate is a time coordinate the values are returned as epoch times for min and max
    # and number of hours for delta.
    #----------------------------------------------------------------------------------------
    def get_min_max_delta(self):
        min_val=self.min_val
        max_val=self.max_val
        delta=self.delta
        is_time, calendar=self.is_time()
        if is_time:
            units=self.attributes[self.units_attrix].value
            # convert the values of this coordinate to epoch times to get min max and delta in hours
            min_val=self.get_epoch_time(self.min_val)
            max_val=self.get_epoch_time(self.max_val)
            if self.nvals>1:
                if self.delta==0:
                    # we had unevenly spaced dates but calculate mean spacing
                    delta_epoch=max_val-min_val
                    delta=delta_epoch/3600 # delta in hours
                    # average delta is this divided by nvals
                    delta=delta/self.nvals
                   
                else:
                    next_date=num2date(self.min_val+self.delta,units=units,calendar=calendar)
                    delta_dates=next_date-num2date(self.min_val,units=units,calendar=calendar)
                    delta=delta_dates.days*24+delta_dates.seconds/3600 # delta in hours

        return min_val, max_val, delta
    

    #----------------------------------------------------------------------------------------
    # function to put min, max and delta values in a string to display converting any time
    # coordinates to datetimes
    # This handles the case where we had no data for time and the value was set to NaN
    # if the coordinate has discrete values rather than evenly spaced values delta apart then
    # include the values in the string too but spread over as many lines as needed so that
    # lines are not too long
    # returns:
    #   this_str - string containing all info
    #   nlines - number of lines the string is over
    #   max_line_len - the number of characters in the longest line
    #   min_val - the min_val which can be used to sort the coordinates by value
    #----------------------------------------------------------------------------------------
    def get_min_max_delta_str(self):
        
        nlines=1
        max_line_len=0
        is_time, calendar= self.is_time()
        if is_time:
            min_val, max_val, delta_hours=self.get_min_max_delta()
            min_date=dt.datetime.fromtimestamp(min_val)
            min_str=min_date.strftime('%Y/%m/%d %H:%M')

            if self.nvals<=1:
                this_str=min_str
                max_line_len=len(this_str)
            else:
                max_date=dt.datetime.fromtimestamp(max_val)
                max_str=max_date.strftime('%Y/%m/%d %H:%M')
                #print('get_min_max_delta_str(): coord', self.cid, self.name, min_str, max_str, delta_hours, 'hours', self.nvals, 'values')
                # check delta time is in sensible units - currently in hours
                delta_units='hours'
                delta_time=delta_hours
                if delta_hours>=24:
                    delta_days=delta_hours/24
                    delta_units='days'
                    delta_time=delta_days
                    if delta_days>=30:
                        delta_months=delta_days/30
                        delta_units='months'
                        delta_time=delta_months
               # if we have monthly data the times will not be evenly spaced but we cannot print all of them
                # we can give range and state every 1 months
                # if this is not likely to be monthly data we can just print the range and aproximate delta
                if self.delta==0 and delta_hours<(24*30):
                    this_str=min_str+' to '+max_str+', {n:d} values approx every {h:.1f}'.format(n=self.nvals, h=delta_time)+' '+delta_units
                else:
                    this_str=min_str+' to '+max_str+', {n:d} values every {h:.1f}'.format(n=self.nvals, h=delta_time)+' '+delta_units
                max_line_len=len(this_str)
        else:
            min_val=self.min_val
            units=''
            if self.units_attrix>=0:
                units=self.attributes[self.units_attrix].value
            if len(self.values)>0:
                # we haven't got a delta so put all values in the string over several lines if necessary
                nlines=0
                this_str=''
                line_str=''
                # put values on lines no wider than 60 chars
                for v in self.values :
                    line_str=line_str+'{v:.2f}, '.format(v=v)
                    this_line_len=len(line_str)
                    if this_line_len>60:
                        this_str=this_str+line_str+'\n'
                        if this_line_len>max_line_len:
                            max_line_len=this_line_len
                        line_str=''
                        nlines=nlines+1
                this_str=this_str+line_str+' '+units
                line_len=len(line_str)
                if line_len>max_line_len:
                    max_line_len=line_len
                nlines=nlines+1
                   
            else:
                if self.nvals==0:
                    this_str='Dimension with no values'
                elif self.nvals==1:
                    this_str='{minv:.2f} '.format(minv=self.min_val)+units
                else:
                    this_str='{minv:.2f} to {maxv:.2f}, {n:d} values, every {delt:.2f} '.format(minv=self.min_val, maxv=self.max_val,n=self.nvals,delt=self.delta)+units
                max_line_len=len(this_str)

        min_max_delta_str=(this_str, nlines, max_line_len, min_val)
        return min_max_delta_str

  
    #----------------------------------------------------------------------------------------
    # print metadata for this coord
    #----------------------------------------------------------------------------------------
    def print(self):
        info_str, nlines, max_line_len, min_val=self.get_min_max_delta_str()
        print('cid=',self.cid, self.name, 'nvals=',self.nvals, info_str)
        for attr in self.attributes:
            print('\t',attr.name,attr.value)


#--------------------------------------------------------------------------------------------------------
# Variable_metadata holds the metadata for a variable
# This handles the cids and fids related to this variable that are stored in Coords_Fids_Of_Variables Table
# In this class we will store a set of cids related to each file for each dimension.
# For variables with no dimensions, a set of fid cid pairs for each fid but with cid=-1 will be stored in the database
# For variables with dimensions that have the same cid for all fids, we will store the cid and fid=-1 to save space in
# the database but we can only do this if there is a dimension for which we will save all the fids (ie there must be
# a multi_dimension.
#---------------------------------------------------------------------------------------------------------
class Variable_metadata:

    max_fids_cids_to_print=8
    
    def __init__(self,*args):
        # args are row, cur and verbose for initiation from database and vid and name for initiation from data
        if isinstance(args[1], str):
            self.init_from_data(*args)
        else:
            self.init_from_database(*args)

    #--------------------------------------------------------------------------------------------
    # Initiate from row which is a single row from a SELECT from the Variable table with all columns
    # Finds the matching attributes for this variable from the Var_Attributes table in the database 
    # Also finds the matching vid, cid, fid rows from the Coords_Fids_Of_Variables table which relates
    # this variable with its coordinates in each file
    # inputs:
    #    row is a single row form the SELECT from Variable table
    #    cur is a cursor on the database
    #-------------------------------------------------------------------------------------------
    def init_from_database(self,row, cur, verbose):
        self.vid=row[0]
        self.name=row[1]
        self.ndims=row[2]
        self.multi_dim=-1 # the dimension which has multiple cids, ie a different one for each fid
        # we need to store the cids for each dimension for each file i.e. cids=[dim0_cids, dim1_cids...]
        # where dim<x>_cids is a list of cids for each dimension and each is of length nfids corresponding
        # to each fid.
        # To save space in the database, if cids for a dimension are the same for all files we store
        # a single cid with matching fid as -1 (but only is there is a dimension with different
        # find the matching coords ids and fids
        res_cids_fids_dimix=cur.execute("""SELECT cid, fid, dimix FROM Coords_Fids_Of_Variables WHERE vid=?""", (self.vid,)).fetchall()
        # cid fid pairs are read out in the order they were put in,
        # i.e. each dimension will be read for all the fids, if the fid=-1 then only 1 cid will be read for that dimension
        cids_fids_dimix_arr =np.asarray(list(map(list,res_cids_fids_dimix)))
        if len(cids_fids_dimix_arr.shape)==2:
            cids=cids_fids_dimix_arr[:,0]
            fids=cids_fids_dimix_arr[:,1]
            dixes=cids_fids_dimix_arr[:,2]
            ix=np.where(fids>=0) # don't use the fids that are -1
            real_fids=np.asarray(fids[ix[0]])
            nfids=len(np.unique(real_fids))
            self.fids=np.zeros(nfids, int)
            if verbose:
                print('Variable_metadata.init_from_database()', self.vid, self.name, 'fids=',fids, 'cids=', cids, 'dixes', dixes) 
            if self.ndims>0:
                self.cids=np.zeros((self.ndims,nfids),int)
                for d in range(self.ndims):
                    ix=np.where(dixes==d)
                    assert(len(ix[0])>0)
                    this_fids=fids[ix[0]]
                    this_cids=cids[ix[0]]
                    if len(this_fids)==1 and this_fids[0]==-1:
                        self.cids[d,:]=this_cids[0]
                        if verbose:
                            print('Variable_metadata.init_from_database()', self.vid, self.name, 'all files have cid', this_cids, 'for dim', d)
                    else:
                        assert(len(this_cids)==nfids)
                        self.cids[d,:]=this_cids
                        if verbose:
                            print('Variable_metadata.init_from_database()', self.vid, self.name, 'cids=', self.cids[d,:], 'for dim', d)
                        if nfids>1:
                            self.multi_dim=d
                        self.fids=this_fids
            else:
                self.cids=cids # will be -1
                self.fids=fids
        else:
            print('Variable_metadata.init_from_database() no fids or cids or dixes! for var', self.vid, self.name)
            pdb.set_trace()
            self.cids=[]
            self.fids=[]
             
        # get the attributes
        self.attributes=[]
        cur.execute("""SELECT name,value FROM Var_Attributes WHERE vid=?""", (self.vid,))
        self.attributes=[Attribute(row_a[0], row_a[1]) for row_a in cur.fetchall()]

    #-----------------------------------------------------------------------------------------------------
    # When initiating from reading data file, adding coordinate ids and fids and attributes is done after
    # initiation.
    # When another file is read with the same variables but different coordinates, we will append that fid
    # and its coordinate cids to the existing variable
    # We don't know at the start how many files this variable will appear in so fids and cids are created as lists.
    #-----------------------------------------------------------------------------------------------------
    def init_from_data(self,vid, name, ndims):
        self.vid=vid
        self.name=name
        self.ndims=ndims
        self.multi_dim=-1
        # we need to store the cids for each dimension for each file i.e. cids=[dim0_cids, dim1_cids...]
        # where dim<x>_cids is a list of cids for each dimension and is of length nfids corresponding
        # to each fid.
        # cids will be set up by add_cids_for_fid()
        # in the end all but one dimension should have the same cid for all files but we can't work this
        # out until all have been added which is when we insert_into_database
        self.cids=[[] for d in range(self.ndims)]
        self.fids=[]
        self.attributes=[]

    #------------------------------------------------------------------------
    # get the fids
    #------------------------------------------------------------------------
    def get_fids(self):
    
        return np.asarray(self.fids)
        
    #---------------------------------------------------------------------------
    # get the number of fids
    #---------------------------------------------------------------------------
    def get_nfiles(self):
         return len(self.fids)

    #------------------------------------------------------------------------
    # add matching coordinate ids for each dimension that are in file fid
    # fid is a single fid, cids is a list/array of length self.ndims
    #------------------------------------------------------------------------
    def add_cids_for_fid(self, fid, cids):

        if self.ndims!=len(cids):
            raise ValueError(f'Variable_metadata.add_cids_for_fid(): invalid length of cids {len(cids)} expected {self.ndims} for {self.name} {self.vid} in file {fid}')
        else:
            if fid in self.fids:
                raise ValueError(f'Variable_metadata.add_cids_for_fid(): cids already exist for {fid} for var {self.name} {self.vid}')
            else:
                #print('Variable_metadata.add_cids_for_fid():',self.name, self.vid, 'adding cids ', cids, 'for fid', fid)
                self.fids.append(fid)
                for d in range(self.ndims):
                    this_cid=cids[d]
                    self.cids[d].append(this_cid)

    #-------------------------------------------------------------------
    # this copies the fid and the cids from other
    # this is used when we have worked out that a new variable is the
    # same as this one from a different file (fid)
    # other will only have one cid for each dimension and one fid
    #-------------------------------------------------------------------
    def copy_fid_cids_from_other(self,other):
        if self.ndims!=other.ndims:
            raise ValueError(f'Variable_metadata.copy_fid_cids_from_other(): cannot copy cids and fids as mismatching ndims for {self.name}')
        else:
            # other will just have one fid and cid for each dimension
            assert(len(other.fids)==1)
            other_cids=[other.cids[d][0] for d in range(other.ndims)]
            self.add_cids_for_fid(other.fids[0],other_cids)
            # check if we now have a multi dimension
            for d in range(self.ndims):
                # are the cids for this dim all the same?
                ncids=len(np.unique(np.asarray(self.cids[d])))
                if ncids>1:
                    if self.multi_dim==-1:
                        self.multi_dim=d
                    elif self.multi_dim!=d:
                        raise ValueError(f'Variable_metadata.copy_fid_cids_from_other(): more than one multi dimension for var {self.name}!')
        

    # returns array of unique cids for dimension d
    def get_cids_for_dim(self,d):
        if isinstance(self.cids, list):
            cids=np.asarray(self.cids[d])
        else:
            cids=self.cids[d]
        if len(cids)>0 and d!=self.multi_dim:
            # this means all the cids are the same
            cids=np.unique(cids)
            assert(len(cids)==1)
        return cids
        
    #--------------------------------------------------------------
    # check whether this variable has same metadata as given variable
    # cids for one dimension may be different but the name of the dimension should be the same
    # if more than one dimension has mismatching values we cannot have more than one multi dimension
    # so other variable can't match self.
    # other is another instance of Variable
    #-------------------------------------------------------------
    def matches_variable(self, other, coords, verbose, thread_name):
        matches=False
        # some attributes can be different in different files and I've even found that sometimes
        # they are string and sometimes float!
        # the ones that should definitely match are:
        must_match_attr_names=['long_name','standard_name','units', 'dataset','statistic', 'time_step', 'var_desc']
        if verbose:
            if len(self.fids)>self.max_fids_cids_to_print:
                fids_str=f'{self.fids[:int(self.max_fids_cids_to_print/2)]}...{self.fids[-int(self.max_fids_cids_to_print/2):]}'
            else:
                fids_str=f'{self.fids}'

        # and the attribute names should all match
        if self.name==other.name and self.ndims==other.ndims:
            matches=True
            # now check the attributes:
            # they must have matching names of attributes and all str attribute values should match but
            # they don't have to be in the same order
            nattr=len(self.attributes)
            nother_attr=len(other.attributes)
            other_attrnames=np.asarray([attr.name for attr in other.attributes])
            if nattr!=nother_attr:
                if verbose:
                    print(thread_name+' Variable_metadata.matches_variable():', self.name, self.vid, 'in files', fids_str, 'other in file', other.fids, 'mismatching number of attributes')
                matches=False
            else:
                file_specific_attribute_indices=[]
                for i in range(nattr):
                    ix=np.where(other_attrnames==self.attributes[i].name)
                    if len(ix[0])==0:
                        matches=False
                        break
                    else:
                        j=ix[0][0]
                        if self.attributes[i].name in must_match_attr_names:
                            # values must match
                            if self.attributes[i].value!=other.attributes[j].value:
                                if verbose:
                                    print(thread_name+' Variable_metadata.matches_variable():',self.name, self.attributes[i].name, 'in files',
                                          fids_str, 'other in file', other.fids, 'attribute does not match', self.attributes[i].value, other.attributes[j].value)
                                matches=False
                                break

                        # if they don't match then mark as file specific but don't change self until all other attributes and coords checked
                        elif self.attributes[i].value!=other.attributes[j].value:
                            file_specific_attribute_indices.append(i)

        if matches and self.ndims>0:
           # check each dimension
           ncids_per_dim=np.zeros(self.ndims,int)
           other_matches_type=np.zeros(self.ndims,int)
           other_matches=np.zeros(self.ndims,int) # exact match of coord
           cids_str='['
           for d in range(self.ndims):
               my_cids=self.get_cids_for_dim(d)
               other_cids=other.get_cids_for_dim(d)
               ncids_per_dim[d]=len(my_cids)
               if d==0:
                   comma=''
               else:
                   comma=', '
               if ncids_per_dim[d]==1 and self.multi_dim>=0:
                   # we know this is not the multi dim
                   cids_str=cids_str+f'{comma} {my_cids}'
                   if len(other_cids)==1 and other_cids[0]==my_cids[0]:
                       other_matches_type[d]=1
                       other_matches[d]=1
                   else:
                       if verbose:
                           print(thread_name+' Variable_metadata.matches_variable():',self.name, self.vid, 'has mismatching cid for non multi dim', d, my_cids, other_cids)
               else:
                   if len(my_cids)>self.max_fids_cids_to_print:
                       this_cids_str=f'{comma} [{my_cids[:int(self.max_fids_cids_to_print/2)]}...{my_cids[-int(self.max_fids_cids_to_print/2):]}'
                   else:
                       this_cids_str=f'{comma} {my_cids}'

                   cids_str=cids_str+this_cids_str

                   # check the coords match even if the cids dont
                   # other_cids should only have one cid as this is the new variable that we are checking against
                   assert(len(other_cids)==1)
                   # if this is the multi dim then the coord should match in all but values and possibly units if it is time
                   other_matches_type[d]=coords[my_cids[0]].matches_coord_type(coords[other_cids[0]], verbose, thread_name)
                   other_matches[d]=(my_cids[0]==other_cids[0])
           nmatches=np.sum(other_matches)
           nmatches_type=np.sum(other_matches_type)
           cids_str=cids_str+' ]'
           # all dims should match coord type
           if nmatches_type<self.ndims:
               if verbose:
                   print(thread_name+' Variable_metadata.matches_variable():',self.name, self.vid, 'does not match a cid in my files=',
                         fids_str,'my cids=', cids_str, 'other vid', other.vid, 'other files=',other.fids, 'other cids',other.cids,
                         'other matches type', other_matches_type, 'other matches exactly', other_matches)
               matches=False
           # we can only have one dim that doesn't match coord values
           if nmatches<self.ndims-1:
               if verbose:
                   print(thread_name+' Variable_metadata.matches_variable():',self.name, self.vid, 'too many mismatching cids in my files=',
                         fids_str,'my cids=', cids_str, 'other vid', other.vid, 'other files=',other.fids, 'other cids',other.cids,                         
                         'other matches type', other_matches_type, 'other matches exactly', other_matches)
               matches=False
                                            
        if matches:
            if self.ndims==0:
                cids_str=f'[{self.cids}]'
            # all important stuff matches but if file_specific attributes don't match set tp FILE_SPECIFIC_VAL
            for i in file_specific_attribute_indices:
                self.attributes[i].value=FILE_SPECIFIC_VAL
            if verbose:
                print(thread_name+' Variable_metadata.matches_variable():',self.name, self.vid, 'in files', fids_str,'cids=', cids_str, 'matches other vid', other.vid, 'in file', other.fids, 'cids=', other.cids)
           
        return matches

    #--------------------------------------------------
    # insert all the variable metadata into the database
    # if the cids for a dimension are the same for all fids we can save space in the database by setting fid=-1
    # and only saving one entry for this cid, however we must have a multi-dimension to do this as we need to
    # store all the fids somewhere
    # if there are no dimenions the cid will be saved as -1 for each fid
    #--------------------------------------------------
    def insert_into_database(self,thread_name,cur,verbose=False):
        if verbose:
            print(thread_name, ' Variable_metadata.insert_into_database(): Creating Variable entry', self.vid, self.name)
        cur.execute("""INSERT INTO Variables (vid, name, ndims) VALUES (?,?,?)""", (self.vid, self.name, self.ndims))
        nfids=self.get_nfiles()

        for d in range(self.ndims):
            this_cids=self.get_cids_for_dim(d)
            if len(this_cids)==1:
                # the cids are the same for all files
                cid=this_cids[0]
                if nfids>1 and self.multi_dim>=0:
                    # make one entry for the unique cid with fid=-1 to indicate all files have the same cid for
                    # this dimension (there must be a multi dim, otherwise we wont save the fids anywhere)
                    # note that entries are in the order of the dimensions
                    fid=-1
                    if verbose:
                        print(thread_name, ' Variable_metadata.insert_into_database(): var={} vid={} creating cid={} fid={} for dimension {}'.format(self.name, self.vid, cid, fid, d))
                    cur.execute("""INSERT INTO Coords_Fids_Of_Variables (vid, cid, fid, dimix) VALUES (?,?,?,?)""", (self.vid, cid, fid, d))
                else:
                    if verbose:
                        print(thread_name, ' Variable_metadata.insert_into_database(): var={} vid={} creating cid={} for fids={} for dimension {}'.format(self.name, self.vid, cid, self.fids, d))
                    for f in range(nfids):
                        fid=self.fids[f]
                        cur.execute("""INSERT INTO Coords_Fids_Of_Variables (vid, cid, fid, dimix) VALUES (?,?,?,?)""", (self.vid, cid, fid, d))
            else:
                # This is the case where there is a different cid for each fid
                if verbose:
                   print(thread_name, ' Variable_metadata.insert_into_database(): var={} vid={} creating cids={} and fids={} for dimension {}'.format(self.name, self.vid, this_cids, self.fids, d))
                for f in range(nfids):
                    cid=this_cids[f]
                    fid=self.fids[f]
                    cur.execute("""INSERT INTO Coords_Fids_Of_Variables (vid, cid, fid, dimix) VALUES (?,?,?,?)""", (self.vid, cid, fid, d))
        if self.ndims==0:
            # this is the case where there are no dimensions so no cids but we need to add the fids with -1 for a cid and dix
            cid=int(-1)
            dix=int(-1)
            if verbose:
                print(thread_name, ' Variable_metadata.insert_into_database(): var={} vid={} creating cid={} for fids={}'.format(self.name, self.vid, cid, self.fids))
            for f in range(nfids):
                fid=self.fids[f]
                cur.execute("""INSERT INTO Coords_Fids_Of_Variables (vid, cid, fid, dimix) VALUES (?,?,?,?)""", (self.vid, cid, fid, dix))
        for att in self.attributes:
           if verbose:
               print(thread_name, ' Variable_metadata.insert_into_database(): Creating attribute for variable', self.vid, self.name, att.name, att.value)
           cur.execute("""INSERT INTO Var_Attributes (vid, name, value) VALUES (?,?,?)""", (self.vid, att.name, att.value))

    #----------------------------------------------------------------------------------------
    # get the dimension which has multiple files and therefore coordinates
    #----------------------------------------------------------------------------------------
    def get_multi_file_dimension(self):
        return self.multi_dim

    #----------------------------------------------------------------------------------------
    # check whether this variable covers all the filters and store the fids that do cover the
    # ranges and are allowed
    # inputs:
    #    fids - only allow the variable fids that are in this list
    #    coord_filters - array of all the filters we may need to check
    #    coords - array of all the coords (note as this is all the coords in the database the cids
    #             are the indices into this array)
    # outputs:
    #    coords_in_range - True/False indicating if whole range is covered for all dimensions
    #    nfids_allowed - the number of files that cover this range
    #----------------------------------------------------------------------------------------
    def check_fids_and_filters(self, fids, coord_filters, coords):

        nfids=self.get_nfiles()
        allowed_fids=np.zeros(nfids, int)
        if len(fids)>0:
            for f in fids:
                fix=np.where(np.asarray(self.fids)==f)
                if len(fix[0])>0:
                    allowed_fids[fix[0][0]]=1
        nfids_allowed=int(sum(allowed_fids))
        coords_in_range=True 
        if len(coord_filters)>0 and nfids_allowed>0:
            for d in range(self.ndims):
                this_cids=self.get_cids_for_dim(d)
                cname=coords[this_cids[0]].name
                # allow filter_name to be part of the coord name eg longitude1 should match with longitude
                matches=np.asarray([cname.find(coord_filter.name) for coord_filter in coord_filters])
                filter_ix=np.where(matches>=0)
                if len(filter_ix[0])>0:
                    if coord_filters[filter_ix[0][0]].min_val!=None or coord_filters[filter_ix[0][0]].max_val!=None:
                        # we need to check this dimension
                        ncids=len(this_cids)
                        if ncids==1:
                            # there is only 1 coord for all files so this coord must cover the range
                            cmin, cmax, cdelta=coords[this_cids[0]].get_min_max_delta()
                        else:
                            # range must be covered by all the cids and need to work out which files are in the range
                            for c in range(ncids):
                                if allowed_fids[c]==1:
                                    this_cmin, this_cmax, cdelta=coords[this_cids[c]].get_min_max_delta()
                                    if coord_filters[filter_ix[0][0]].min_val!=None:
                                        if this_cmax<coord_filters[filter_ix[0][0]].min_val:
                                            # dont need this as it ends before min required
                                            allowed_fids[c]=0
                                    if coord_filters[filter_ix[0][0]].max_val!=None:
                                        if this_cmin>coord_filters[filter_ix[0][0]].max_val:
                                            # dont need this as it starts after max required
                                            allowed_fids[c]=0
                                    # find overall min and max
                                    if c==0:
                                         cmin=this_cmin
                                         cmax=this_cmax
                                    else:
                                         cmin=np.amin([cmin,this_cmin])
                                         cmax=np.amax([cmax,this_cmax])

                        #check that fids cover the whole range
                        if coord_filters[filter_ix[0][0]].min_val!=None:
                            if cmin>coord_filters[filter_ix[0][0]].min_val:                
                                coords_in_range=False    
                        if coord_filters[filter_ix[0][0]].max_val!=None:
                            if cmax<coord_filters[filter_ix[0][0]].max_val:
                                coords_in_range=False
        # save the allowed_fids
        self.allowed_fids=allowed_fids
        nfids_allowed=int(sum(allowed_fids))
        return coords_in_range, nfids_allowed


    #---------------------------------------------------------------------------
    # return the attributes as a string with each attribute on a separate line
    # also returns the max length of the lines
    #---------------------------------------------------------------------------
    def get_attributes_str(self):

        attr_str=''
        max_line_len=0
        for attr in self.attributes:
            this_attr_str=attr.get_attr_str()
            if len(this_attr_str)>max_line_len:
                 max_line_len=len(this_attr_str)
            attr_str=attr_str+this_attr_str
        return attr_str, max_line_len

    #---------------------------------------------------------------------------
    # print info
    #---------------------------------------------------------------------------
    def print(self):
        print('vid=',self.vid, self.name, 'ndims=', self.ndims, 'nfids=',len(self.fids))
        if len(self.fids)<5:
            print('\tfids=', self.fids)
        for d in range(self.ndims):
            print('\tdim', d, ': cid=', self.get_cids_for_dim(d))
        for attr in self.attributes:
            print('\t'+attr.name, attr.value)



#----------------------------------------------------------------------------------------------------------
# read all the directories table entries and return dirpaths as a list, the index into which is the did
#---------------------------------------------------------------------------------------------------------
def read_all_directories(cur):
    ndirs=0
    dirpaths=[]
    res=cur.execute("""SELECT did,dirpath FROM Directories""")
    for row in res.fetchall():
        this_dir=Directory(row)
        if this_dir.did!=ndirs:
            raise ValueError(f'read_all_directories(): unexpected directory id {this_dir.did}')

        dirpaths.append(this_dir.dirpath)
        ndirs=ndirs+1

    return dirpaths

#-----------------------------------------------------------
# functions to select certain rows of variables and coords
#-----------------------------------------------------------
def select_all_variables(cur, order=False):
    if order:
        res=cur.execute("""SELECT vid,name,ndims FROM Variables ORDER BY name""")
    else:
        res=cur.execute("""SELECT vid,name,ndims FROM Variables""")
    return res.fetchall()

def select_variables_by_name(name,cur):
    res=cur.execute("""SELECT vid,name,ndims FROM Variables WHERE name=?""", (name,))
    return res.fetchall()

def select_all_cid_fid_of_variables(cur):
    res=cur.execute("""SELECT vid, cid, fid FROM Coords_Fids_Of_Variables""")
    return res.fetchall()

def select_cid_fid_of_variables_by_vid(cur,vid):
    sql="SELECT vid, cid, fid FROM Coords_Fids_Of_Variables WHERE vid=?"
    res=cur.execture(sql, (vid,))
    return res.fetchall()

def select_all_coords(cur):
    res=cur.execute("""SELECT cid, name, nvals, min_val, max_val, delta FROM Coords""")
    return res.fetchall()
    
def select_all_coords_like_name(cur,name, fetch_one):
    res_coords=cur.execute("""SELECT cid, name, nvals, min_val, max_val, delta FROM Coords WHERE name LIKE ?""", (name+'%',))
    if fetch_one:
        rows=res_coords.fetchone()
    else:
        rows=res_coords.fetchall()
    return rows
