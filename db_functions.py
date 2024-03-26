'''
    Code for all the database related functions:

    Defines classes that hold data related to database tables and handle reading/writing the relevant data
    from/to the database

    These functions are called by both build_metadata_db.py and metaview.py

'''

import pdb
import sqlite3
from netCDF4 import num2date, date2num
import numpy as np
import datetime as dt
import os

FILE_SPECIFIC_VAL='File specific'  # used to set the value of an attribute that we don't really care about
                                   # and is different for different variables in different files

#--------------------------------------------------------------------------------------------
# create all the tables we need to store metadata
#--------------------------------------------------------------------------------------------
def create_tables(cur,verbose=False):
    if verbose:
        print('Creating tables')
    cur.execute("CREATE TABLE Files(fid INTEGER PRIMARY KEY, dirname TEXT, filename TEXT, symlink TEXT, created REAL, modified REAL)")
    cur.execute("CREATE TABLE Global_Attributes(fid INTEGER, name TEXT, value)")
    cur.execute("CREATE TABLE Coords(cid INTEGER PRIMARY KEY, name TEXT, nvals INTEGER, min_val REAL, max_val REAL, delta REAL)")
    cur.execute("CREATE TABLE Discrete_Coord_Values(cid INTEGER, value REAL)")
    cur.execute("CREATE TABLE Coord_Attributes(cid INTEGER, name TEXT, value)")
    cur.execute("CREATE TABLE Variables(vid INTEGER PRIMARY KEY, name TEXT, ndims INTEGER)")
    cur.execute("CREATE TABLE Coords_Fids_Of_Variables(vid INTEGER, cid INTEGER, fid INTEGER)")
    cur.execute("CREATE TABLE Var_Attributes(vid INTEGER, name TEXT, value)")

#-----------------------------------------------------------------
# function to combine a directory and filename to give a filepath
#----------------------------------------------------------------
def get_filepath(dirname, filename):
    return dirname+'/'+filename


#--------------------------------------------------------------------------------------------
# define classes to hold data read from database and the code to read them from the database
#--------------------------------------------------------------------------------------------

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
        if isinstance(value, str)==False:
            value=float(value)
        self.value=value

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
    # get the global attributes from the database (use if init_from_database hasn't read them
    #---------------------------------------------------------------------------------------
    def read_global_attributes(self, cur):
        # select all data from Global_Attributes table
        cur.execute("""SELECT name, value FROM Global_Attributes WHERE fid=?""",(self.fid,))
        for row in cur.fetchall():
           self.global_attributes.append(Attribute(row[0],row[1]))
            
    #---------------------------------------------------------------------------------------
    # initiation from database where row is value returned from a fetchall() after a
    # SELECT statement for all values from the Files table
    # cur is the cursor to the database so we can get matching global attributes
    # we wont read the global attributes at this point - use read_global_attributes() to read them 
    #---------------------------------------------------------------------------------------
    def init_from_database(self,row, cur):
        self.fid=row[0]
        self.dirname=row[1]
        self.filename=row[2]
        self.symlink=row[3]
        self.created=row[4]
        self.modified=row[5]
        self.global_attributes=[]

    #---------------------------------------------------------------------------------------
    # initiation when creating database. This sets up all but the global attributes
    # global attributes can be added by calling add_attribute()
    #---------------------------------------------------------------------------------------
    def init_from_data(self, fid, dirname, filename):
        self.fid=fid
        self.dirname=dirname
        self.filename=filename
        self.symlink=''
        # check whether this file is a symbolink link and what its creation and modification dates are
        my_filepath=self.get_filepath()
        if os.path.islink(my_filepath):
            self.symlink=os.readlink(my_filepath)
            print(my_filepath, 'symlink', self.symlink)
        self.created=os.path.getctime(my_filepath)
        self.modified=os.path.getmtime(my_filepath)
        self.global_attributes=[]

    #---------------------------------------------------------------------------------------
    # called after init_from_data to add attributes
    # name should vbe a str
    # value can be a str or int or float
    #---------------------------------------------------------------------------------------
    def add_attribute(self, name, value):
        self.global_attributes.append(Attribute(name,value))

    #---------------------------------------------------------------------------------------
    # combine my dirname and filename
    #---------------------------------------------------------------------------------------
    def get_filepath(self):
        return get_filepath(self.dirname, self.filename)

    #---------------------------------------------------------------------------------------
    # inserts file data into Files table and any global attributes into Global_Attributes table
    #---------------------------------------------------------------------------------------
    def insert_into_database(self, cur,verbose=False):
        # create an entry in Files table for this file
        if verbose:
            print('Creating File entry', self.fid, self.get_filepath())
        cur.execute("""INSERT INTO Files (fid, dirname, filename, symlink, created, modified) VALUES(?,?,?,?,?,?)""",
                   (self.fid, self.dirname, self.filename, self.symlink, self.created, self.modified))
        for att in self.global_attributes:
            cur.execute("""INSERT INTO Global_Attributes(fid, name, value) VALUES (?,?,?)""",
                        (self.fid, att.name, att.value))


    #--------------------------------------------------------------------------------------------------
    # combine the file information into a string separated into different lines for different bits of
    # information
    # this does not include the global attributes
    # returns:
    #   the combined string
    #   nlines - number of lines in the string
    #   max_line_len - the number of characters in the longest line
    #--------------------------------------------------------------------------------------------------
    def get_file_info_str(self):
        created_date = dt.datetime(1970,1,1)+dt.timedelta(seconds=self.created)
        modified_date = dt.datetime(1970,1,1)+dt.timedelta(seconds=self.modified)
        file_text=self.get_filepath()+'\n'
        sym_text='symlink='+self.symlink+'\n'
        create_text='created='+created_date.strftime('%Y-%m-%d %H:%M')+'\n'
        mod_text='modified='+modified_date.strftime('%Y-%m-%d %H:%M')
        max_line_len=np.amax([len(file_text),len(sym_text),len(create_text),len(mod_text)])
        nlines=4
        return file_text+sym_text+create_text+mod_text,nlines,max_line_len

    #---------------------------------------------------------------------------------------
    # function to print info about this file including the attributes
    #---------------------------------------------------------------------------------------
    def print(self):
        print(self.fid, self.get_filepath(), self.symlink, self.created, self.modified)
        for att in self.global_attributes:
            print('\t', att.name, att.value)

#--------------------------------------------------
# Files_metadata holds the data for all files
#--------------------------------------------------
class Files_metadata:

    #---------------------------------------------------------------------------------------
    # initiation from database where cur is a cursor for the database
    #---------------------------------------------------------------------------------------
    def __init__(self,cur):
        self.all_files_metadata=[]
        # select all data from Files table
        cur.execute("""SELECT fid, dirname, filename, symlink, created, modified FROM Files""")
        for row in cur.fetchall():
            self.all_files_metadata.append(File_metadata(row, cur))
            
    #---------------------------------------------------------------------------------------
    # returns number of files stored
    #---------------------------------------------------------------------------------------
    def get_nfiles(self):
        return len(self.all_files_metadata)

    #---------------------------------------------------------------------------------------
    # gets the dirnames from each file and finds the unique ones
    #---------------------------------------------------------------------------------------
    def get_unique_dirnames(self):
        dirnames=np.asarray([this_file.dirname for this_file in self.all_files_metadata])
        return np.unique(dirnames)
            
    #---------------------------------------------------------------------------------------
    # returns a list of the fids of the files that have a dirname that equals the dirname requested
    #---------------------------------------------------------------------------------------
    def get_fids_for_matching_dirnames(self,dirname):
        matches=[this_file.dirname==dirname for this_file in self.all_files_metadata]
        ix=np.where(matches)
        fids=[self.all_files_metadata[f].fid for f in ix[0]]
        return fids
    
    #---------------------------------------------------------------------------------------
    # returns the file_metadata for the file with an fid that matches that given, (None if not found)
    # if all the files have been read from the database then the fid is the same as the index into the array
    #---------------------------------------------------------------------------------------
    def get_matching_fid(self,fid):
        this_file=None
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
                self.min_val=dt.datetime.strptime(min_val, '%Y-%m-%d')
            else:
                self.min_val=float(min_val)

        max_val=self.max_widget.get()
        if max_val=='':
            self.max_val=None
        else:
            if self.is_time:
                self.max_val=dt.datetime.strptime(max_val, '%Y-%m-%d')
            else:
                self.max_val=float(max_val)

# Coord_metadata holds the data that is in the Coord table
class Coord_metadata:

    def __init__(self,*args):
        # if two args these are row and cur for initiation from database otherwise if 3 are
        #given this is initiation when creating database 
        if len(args) == 2: 
            self.init_from_database(*args)
        else:
            self.init_from_data(*args)

    #-------------------------------------------------------------------------------------------------------
    # initiate from database where row is a single row from a SELECT from the Coord table with all columns
    # finds the matching attributes for this coordinate from the Coord_Attributes table in the database 
    # if the coordinate values were not evenly spaced eg pressure levels, the actual values can be
    # found in the Discrete_Coord_Values table 
    # inputs:
    #    row is a single row form the SELECT from Coord table
    #    cur is a cursor on the database
    #-------------------------------------------------------------------------------------------------------
    def init_from_database(self,row, cur):
        self.cid=row[0]
        self.name=row[1]
        self.nvals=row[2]
        self.min_val=row[3]
        self.max_val=row[4]
        self.delta=row[5]
        self.values=[]   # used to hold discrete values if there are any
        self.attributes=[]
        self.units_attrix=-1
        self.calendar_attrix=-1
        # find the attributes of this coordinate
        cur.execute("""SELECT name,value,cid FROM Coord_Attributes WHERE cid=?""", (self.cid,))
        attr_rows=cur.fetchall()
        i=0
        for attr in attr_rows:
            self.add_attribute(attr[0], attr[1])
            i=i+1
        if self.delta==0:
            cur.execute("""SELECT value FROM Discrete_Coord_Values WHERE cid=?""", (self.cid,))
            val_rows=cur.fetchall()
            for val in val_rows:
                self.values.append(val[0])

    #--------------------------------------------------
    # initiate from reading datafile
    #--------------------------------------------------
    def init_from_data(self,cid, name, coord_values):
        self.cid=cid
        self.name=name
        self.delta=0
        self.nvals=len(coord_values)
        self.values=[]

        # sqlite3 doesn't handle integers well - it creates blobs depending on size of integer so best
        # to save data as float
        self.min_val=float(np.amin(coord_values))
        self.max_val=float(np.amax(coord_values))

        if self.nvals>1:
            deltas=abs(coord_values[1:]-coord_values[:-1])
            if abs(deltas[0]-deltas[-1])<00.0001:
                self.delta=float(np.mean(deltas))
            else:
                self.values=[float(v) for v in coord_values]

        self.attributes=[] # attributes are added by calling add_attribute()
        self.units_attrix=-1
        self.calendar_attrix=-1

    #--------------------------------------------------
    # add an attrbute when creating from data file
    #--------------------------------------------------
    def add_attribute(self, name, value):
        self.attributes.append(Attribute(name,value))
        if name=='units':
            self.units_attrix=len(self.attributes)-1
        elif name=='calendar':
            self.calendar_attrix=len(self.attributes)-1

    #----------------------------------------------------------------------------------------
    # this coordinate is a time coordinate if it has units and calendar attributes
    #----------------------------------------------------------------------------------------
    def is_time(self):
        return self.units_attrix>=0 and self.calendar_attrix>=0

    #----------------------------------------------------------------------------------------
    # check whether this coord has same metadata as given coord
    # cid doesn't matter but other values should match
    # also both should have matching attribute names and values
    # other is another instance of coord
    #---------------------------------------------------------------------------------------
    def matches_coord(self, other):
        matches=False
        if self.name==other.name and self.nvals==other.nvals and abs(self.min_val-other.min_val)<1e-6 and  abs(self.max_val-other.max_val)<1e-6 and abs(self.delta-other.delta)<1e-6: 
            if len(self.values)>0:
                if len(self.values)==len(other.values):
                    matches=True
            else:
                matches=True

        # now check the attributes - they must both match
        if matches:
            nattr=len(self.attributes)
            nother_attr=len(other.attributes)
            if nattr!=nother_attr:
                matches==False
            else:
                for i in range(nattr):
                    if self.attributes[i].name!=other.attributes[i].name or self.attributes[i].value!=other.attributes[i].value:
                        matches=False
                        break

        return matches

    #----------------------------------------------------------------------------------------
    # code to insert coord into Coords table, any values into Discrete_Coord_Values table
    # and any attributes into Coords_Attributes table
    # cur is the cursor for the database
    #----------------------------------------------------------------------------------------
    def insert_into_database(self, cur,verbose=False):
        # create Coord entry
        if verbose:
            print('creating Coord entry', self.cid, self.name, self.nvals, self.min_val, self.max_val, self.delta)
        cur.execute("""INSERT INTO Coords (cid, name, nvals, min_val, max_val, delta) VALUES (?,?,?,?,?,?)""",
                    (self.cid, self.name, self.nvals, self.min_val, self.max_val, self.delta))
        if len(self.values)>0:
            for i in range(self.nvals):
                cur.execute("""INSERT INTO Discrete_Coord_Values (cid, value) VALUES (?,?)""", (self.cid, self.values[i]) )
        if len(self.attributes)>0:
            for att in self.attributes:
                cur.execute("""INSERT INTO Coord_Attributes (cid, name, value) VALUES (?,?,?)""", (self.cid, att.name, att.value))
                
    #----------------------------------------------------------------------------------------
    # this function returns the min_val, max_val and delta values of this coordinate converted
    # to datetime objects and the delta to a number of hours if this is a time coord.
    #----------------------------------------------------------------------------------------
    def get_min_max_delta(self):
        min_val=self.min_val
        max_val=self.max_val
        delta=self.delta
        if self.is_time():
            calendar=self.attributes[self.calendar_attrix].value
            units=self.attributes[self.units_attrix].value
            units_split=units.split(' ')
            if units_split[0]!='hours' and units_split[0]!='days':
                print('unexpected units for time', units)
                pdb.set_trace()
            # convert the values of this coordinate to dates to get min max and delta in hours
            min_val=num2date(self.min_val,units=units,calendar=calendar)
            max_val=num2date(self.max_val,units=units,calendar=calendar)
            if self.nvals>1:
                next_date=num2date(self.min_val+self.delta,units=units,calendar=calendar)
                delta_dates=next_date-min_val
                delta=delta_dates.days*24+delta_dates.seconds/3600 # delta in hours
        return min_val, max_val, delta
    

    #----------------------------------------------------------------------------------------
    # function to put min, max and delta values in a string to display converting any time
    # coordinates to datetimes
    # if the coordinate has discrete values rather than evenly spaced values delta apart then
    # include the values in the string too but spread over as many lines as needed so that
    # lines are not too long
    # returns:
    #   this_str - string containing all info
    #   nlines - number of lines the string is over
    #   max_line_len - the number of characters in the longest line
    #----------------------------------------------------------------------------------------
    def get_min_max_delta_str(self):
        nlines=1
        max_line_len=0
        if self.is_time():
            min_date, max_date,delta_hours=self.get_min_max_delta()
            min_str=min_date.strftime('%Y/%m/%d %H:%M')
            max_str=max_date.strftime('%Y/%m/%d %H:%M')
            this_str=min_str+' to '+max_str+' every {h:.2f}'.format(h=delta_hours)+' hours'
            max_line_len=len(this_str)
        else:
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
                this_str='{minv:.2f} to {maxv:.2f} every {delt:.2f} '.format(minv=self.min_val, maxv=self.max_val,delt=self.delta)+units
                max_line_len=len(this_str)

        return this_str, nlines, max_line_len

  
    #----------------------------------------------------------------------------------------
    # print metadata for this coord
    #----------------------------------------------------------------------------------------
    def print(self):
        info_str, nlines, max_line_len=self.get_min_max_delta_str()
        print(self.cid, self.name, 'nvals=',self.nvals, info_str)
        for attr in self.attributes:
            print('\t',attr.name,attr.value)


#--------------------------------------------------
# Variable_metadata holds the metadata for a variable
#--------------------------------------------------
class Variable_metadata:

    def __init__(self,*args):
        # args are row and cur for initiation from database and vid and name for initiation from data
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
    def init_from_database(self,row, cur):
        self.vid=row[0]
        self.name=row[1]
        self.ndims=row[2]
        self.cids=[] # the cids that go with each fid for each dim - if all fids have the same cid then there will be only one entry for that dim
        for n in range(self.ndims):
            self.cids.append([])
        self.fids=[]
        self.attributes=[]
        # find the matching coords ids and fids
        res_cids_fids=cur.execute("""SELECT cid, fid FROM Coords_Fids_Of_Variables WHERE vid=?""", (self.vid,)).fetchall()
        nrows=len(res_cids_fids)
        nfids_expected=nrows+1-self.ndims
        # cid fid pairs are read out in the order they were put in,
        # i.e. each dimension will be read for all the fids, if the fid=-1 then only 1 cid will be read for that dimension
        has_one_cid=np.zeros(self.ndims,int)
        dimix=0

        for cid_fid in res_cids_fids:
            this_cid=cid_fid[0]
            this_fid=cid_fid[1]
            if this_fid==-1:
                has_one_cid[dimix]=1
            else:
                new_fid=self.add_fid(this_fid)

            self.add_cid(dimix, this_cid)
            if has_one_cid[dimix]==1 or self.get_nfiles()==nfids_expected:
                dimix=dimix+1

        self.fids=np.asarray(self.fids)

        # get the attributes
        cur.execute("""SELECT name,value FROM Var_Attributes WHERE vid=?""", (self.vid,))
        for row_a in cur.fetchall():
            self.attributes.append(Attribute(row_a[0], row_a[1]))

    #-----------------------------------------------------------------------------------------------------
    # initiation from reading data file, adding coordinate ids and fids and attributes is done after initiation
    #-----------------------------------------------------------------------------------------------------
    def init_from_data(self,vid, name,ndims):
        self.vid=vid
        self.name=name
        self.ndims=ndims
        # we need to store the cids for each dimension for each file i.e. cids=[dim1_cids, dim2_cids...]
        # where dim<x>_cids is a list of cids corresponding to each fid for a particular dimension
        # cids and fids will be set up by add_fid() and add_cid()
        # in the end all but one dimension should have the same cid for all files but we can't work this
        # out until all have been added which is when we insert_into_database
        self.cids=[]  
        for n in range(self.ndims):
            self.cids.append([])
        self.fids=[]
        self.attributes=[]

    #---------------------------------------------------------------------------
    # get the number of fids
    #---------------------------------------------------------------------------
    def get_nfiles(self):
         return len(self.fids)

    #---------------------------------------------------------
    # add fid to list if not there
    #---------------------------------------------------------
    def add_fid(self, fid):
        new_fid=False
        # add the fid if not there
        ix=np.where(np.asarray(self.fids)==fid)
        if len(ix[0])==0:
            self.fids.append(fid)
            new_fid=True

        return new_fid

    #---------------------------------------------------------
    # add a matching coordinate id for dimension dimix
    #---------------------------------------------------------
    def add_cid(self, dimix, cid):

        if self.ndims<dimix+1:
            print('invalid dimix', dimix, 'this var has', self.ndims, 'dimensions')
            pdb.set_trace()
        else:
            self.cids[dimix].append(cid)

    #-------------------------------------------------------------------
    # this copies the fid and the cids from other
    # this is used when we have worked out that a new variable is the
    # same as this one from a different file (fid)
    #-------------------------------------------------------------------
    def copy_fid_cids_from_other(self,other):
        if self.ndims!=other.ndims:
            print('cannot copy cids and fids as mismatching ndims')
        else:
            for fix in range(other.get_nfiles()):  # usually other will just have one fid and cid for each dimension
                self.add_fid(other.fids[fix])
                for d in range(self.ndims):
                   self.add_cid(d,other.cids[d][fix])                
        
    #--------------------------------------------------
    # add an attribute when creating from data file
    #--------------------------------------------------
    def add_attribute(self, name, value):
        self.attributes.append(Attribute(name,value))

    #--------------------------------------------------------------
    # check whether this variable has same metadata as given variable
    # other is another instance of Variable
    #-------------------------------------------------------------
    def matches_variable(self, other):
        matches=False
        if self.name==other.name:
            matches=True 
            # now check the attributes:
            # they must having matching names of attributes
            # the follwing ones shoud also have matching values
            attrnames_to_match=['units', 'long_name','standard_name']
            nattr=len(self.attributes)
            nother_attr=len(other.attributes)
            if nattr!=nother_attr:
                matches==False
            else:
                for i in range(nattr):
                    if self.attributes[i].name!=other.attributes[i].name:
                        matches=False
                        break
                    else:
                        if self.attributes[i].name in attrnames_to_match:
                            # values must match
                            if self.attributes[i].value!=other.attributes[i].value:
                                matches=False
                                break
                        else:
                            # don't care if values match but mark as files specific if they dont
                            if self.attributes[i].value!=other.attributes[i].value:
                                self.attributes[i].value=FILE_SPECIFIC_VAL
        # now we need to check the coordinates - to be the same variable, only one coordinate can be different, eg time
        if self.ndims!=other.ndims:
             matches=False
        else:
           # check each dimension
           ncids_per_dim=np.zeros(self.ndims,int)
           other_matches=np.zeros(self.ndims,int)
           for d in range(self.ndims):
               my_cids=np.unique(np.asarray(self.cids[d]))
               other_cids=np.unique(np.asarray(other.cids[d]))
               ncids_per_dim[d]=len(my_cids)
               if ncids_per_dim[d]==1:
                   if len(other_cids)==1 and other_cids[0]==my_cids[0]:
                       other_matches[d]=1
           nmatches=np.sum(other_matches)
           # we should have at least all dimensions-1 matching and the non matching dim should be the same for all fids
           if nmatches<self.ndims-1:
               matches=False
           
        return matches

    #--------------------------------------------------
    # insert all the variable metadata into the database
    #--------------------------------------------------
    def insert_into_database(self,cur,verbose=False):
        if verbose:
            print('Creating Variable entry', self.vid, self.name)
        cur.execute("""INSERT INTO Variables (vid, name, ndims) VALUES (?,?,?)""", (self.vid, self.name, self.ndims))
        for d in range(self.ndims):
            this_cids=np.unique(np.asarray(self.cids[d]))
            if len(this_cids)==1 and len(self.fids)>1:
                # make one entry for the unique cid with fid=-1 to indicate all files have the same cid for this dimension
                # note that entries are in the order of the dimensions
                fid=-1
                cid=self.cids[d][0] # this must be type <int> not <int64> to work in the database
                if verbose:
                    print(self.vid, 'creating vid cid fid for dim', self.vid, cid, fid, d)
                cur.execute("""INSERT INTO Coords_Fids_Of_Variables (vid, cid, fid) VALUES (?,?,?)""", (self.vid, cid, fid))
            else:
                for f in range(self.get_nfiles()):
                    cid=self.cids[d][f]
                    fid=self.fids[f]
                    if verbose:
                        print(self.vid, 'creating vid cid fid for dim', self.vid, cid, fid, d)
                    cur.execute("""INSERT INTO Coords_Fids_Of_Variables (vid, cid, fid) VALUES (?,?,?)""", (self.vid, cid, fid))
        for att in self.attributes:
            cur.execute("""INSERT INTO Var_Attributes (vid, name, value) VALUES (?,?,?)""", (self.vid, att.name, att.value))

    #----------------------------------------------------------------------------------------
    # get the dimension which has multiple files and therefore coordinates
    #----------------------------------------------------------------------------------------
    def get_multi_file_dimension(self):
        d=-1
        ncids=np.asarray([len(cids) for cids in self.cids])
        dix=np.where(ncids>1)
        if len(dix[0])==0:
            print('cannot find dimension with more than 1 cid')
            pdb.set_trace()
        else:
            d=dix[0][0]

        return d

    #----------------------------------------------------------------------------------------
    # check whether this variable covers all the filters and store the fids that do cover the
    # ranges and are allowed
    # inputs:
    #    fids - if [] then there is no restriction on the fids otherwise only look at the variable
    #           fids that are in this list
    #    coord_filters - array of all the filters we may need to check
    #    coords - array of all the coords (note as this is all the coords in the database the cids
    #             are the indices into this array)
    # outputs:
    #    coords_in_range - True/False indicating if whole range is covered for all dimensions
    #    nallowed_fids - the number of files that cover this range
    #----------------------------------------------------------------------------------------
    def check_fids_and_filters(self, fids, coord_filters, coords):

        filter_names=np.asarray([coord_filter.name for coord_filter in coord_filters])

        if len(fids)==0:
            allowed_fids=np.ones(self.get_nfiles())
        else:
            allowed_fids=np.zeros(self.get_nfiles())
            for f in fids:
                fix=np.where(self.fids==f)
                if len(fix[0])>0:
                    allowed_fids[fix[0][0]]=1

        coords_in_range=True 
        if len(filter_names)>0:       
            for d in range(self.ndims):
                cname=coords[self.cids[d][0]].name
                filter_ix=np.where(filter_names==cname)
                if len(filter_ix[0])>0:
                    if coord_filters[filter_ix[0][0]].min_val!=None or coord_filters[filter_ix[0][0]].max_val!=None:
                        # we need to check this dimension
                        ncids=len(self.cids[d])
                        if ncids==1:
                            # there is only 1 coord for all files so this coord must cover the range
                            cmin, cmax, cdelta=coords[self.cids[d][0]].get_min_max_delta()
                        else:
                            # range must be covered by all the cids and need to work out which files are in the range
                            for c in range(ncids):
                                if allowed_fids[c]==1:
                                    this_cmin, this_cmax, cdelta=coords[self.cids[d][c]].get_min_max_delta()
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

        self.allowed_fids=allowed_fids
        return coords_in_range, int(np.sum(allowed_fids))


    #---------------------------------------------------------------------------
    # return the attributes as a string with each attribute on a separate line
    # also returns the max length of the lines
    #---------------------------------------------------------------------------
    def get_attributes_str(self):

        attr_str=''
        max_line_len=0
        for attr in self.attributes:
            this_attr_str=attr.name+' : '
            if isinstance(attr.value, str):
                this_attr_str=this_attr_str+attr.value
            else:
                if attr.value<0.01:
                    this_attr_str=this_attr_str+'{v:.2g}'.format(v=attr.value)
                else:
                    this_attr_str=this_attr_str+'{v:.2f}'.format(v=attr.value)
            attr_str=attr_str+this_attr_str+'\n'
            if len(this_attr_str)>max_line_len:
                 max_line_len=len(this_attr_str)
        return attr_str, max_line_len

    #---------------------------------------------------------------------------
    # print info
    #---------------------------------------------------------------------------
    def print(self):
        print(self.vid, self.name,  'fids=',self.fids, 'cids=', self.cids)
        for attr in self.attributes:
            print('\t'+attr.name, attr.value)



#-----------------------------------------------------------
# functions to select certain rows of variables and coords
#-----------------------------------------------------------
def select_all_variables(cur):
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
