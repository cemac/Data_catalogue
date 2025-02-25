
'''
    Code to display a GUI to explore the database(s) created by build_metdata_db.py

    Usage: python metaview.py <dbname or root directory in which to find dbs> [coord1 coord2...] -v

    If the user gives a single database name (ends in .db) then we just open that database but if they
    give a directory name we will search the directory for anything with a .db extension and create
    a Database_reader which opens the database. We find the list of directories and unique variables
    that are found in these databases.
    The coord1 coord2 etc are the names of coordinates that we can filter on

'''
import sys
import os
from tkinter import *
from tkinter import messagebox
import warnings
import pdb
import sqlite3
from db_functions import *       

# set default font for Labels and Text
font=('Ariel', 11)

# global variables
databases=[]
unique_dirnames=[]
unique_varnames=[]
verbose=False
coord_filters=[]
current_db=-1 # index to current database set by dirname which is initially all

# only update the status every UPDATE_COUNT times round a loop otherwise it slows things down too much
UPDATE_COUNT=100
def update_status(text):
    status_bar['text']=text
    root.update_idletasks()
    
#--------------------------------------------
# Class to handle a single database
#--------------------------------------------
class Database_reader:

    def __init__(self,dbname,verbose):
        # open the database
        if verbose:
            print('opening database', dbname)
        try:

            self.con = sqlite3.connect(dbname)
        except OSError as err:
            warnings.warn('Cannot read database {dbname}, error={err}'.format(dbname=dbname, err=err),UserWarning)

        self.cur = self.con.cursor()
        self.dbname=dbname

        #-----------------------------------------------------------------------------------
        # get a list of all directory names from database
        #-----------------------------------------------------------------------------------
        self.dirpaths=read_all_directories(self.cur)
        if verbose:
            print(dbname, 'directories:', self.dirpaths)

        #-----------------------------------------------------------------------------------
        # get list of all unique variable names from database - we only need name at this stage
        #-----------------------------------------------------------------------------------
        self.unique_varnames=[]
        res=self.cur.execute("""SELECT name FROM Variables""")
        all_varnames=np.asarray(res.fetchall())
        # just get the unique varnames
        for v in np.unique(all_varnames):
            self.unique_varnames.append(v)

        #-----------------------------------------------------------------------------------
        # have a place to store coordinates, variables and files that have been searched for
        #-----------------------------------------------------------------------------------
        # place to store coords but only read when needed
        self.coords=[]
        # also store the stuff we get back from get_min_max_delta_str()
        self.coords_str=[]
        self.coords_nlines=[]
        self.coords_max_line_len=[] 
        self.coords_min_vals=[]
        
        # place to store the files- initially no files but read on a search        
        self.files_metadata=Files_metadata()
        # hold the variables that were searched for here - initially empty
        self.active_variables=[]

    def has_dirpath(self,dirpath):
         matches=np.asarray([this_dir==dirpath for this_dir in self.dirpaths])
         ix=np.where(matches)
         return len(ix[0])

    def get_did(self,dirpath):
         matches=np.asarray([this_dir==dirpath for this_dir in self.dirpaths])
         ix=np.where(matches)
         if len(ix[0])!=1:
             raise ValueError('DatabaseReader.get_did() database does not have one matching directory '+self.dbname)
         return int(ix[0][0])

    # read one coordinate with a name like coord_filter.name and see if it is_time coordinate
    def coordinate_filter_is_time(self, coord_filter):
        row=select_all_coords_like_name(self.cur, coord_filter.name, True) # just get one coord
        is_time=False
        if row==None:
            print(coord_filter.name, 'does not match any coordinate')
            coord_filter.is_valid=False
        else:
            coord=Coord_metadata(row, self.cur)
            is_time,calendar=coord.is_time()
        return is_time
        
    def read_variables(self,variable,verbose):
        if variable=='*':
            # we are looking for all variables
            var_rows=select_all_variables(self.cur,True) # order them
        else:
            # we are looking for a specific variable
            var_rows=select_variables_by_name(variable,self.cur)
        if verbose:
            print('Database_reader.read_variables()', variable)
        nvars=len(var_rows)
        update_status('reading variables ({}) 0/{}'.format(variable, nvars))
        self.active_variables=[None]*nvars # create list of required size to hold variables
        r=0
        for row in var_rows:
            self.active_variables[r]=Variable_metadata(row, self.cur)
            if r % UPDATE_COUNT ==0:
                update_status('reading variables ({}) {}/{}'.format(self.active_variables[r].name, r,nvars))
            r+=1
        update_status('')
        return nvars


    def create_coord(self, row, ncoords):
        c=self.coord_counter
        self.coords[c]=Coord_metadata(row, self.cur)
        (this_str, nlines, max_line_len, min_val)=self.coords[c].get_min_max_delta_str()
        self.coords_str[c]=this_str
        self.coords_nlines[c]=nlines
        self.coords_max_line_len[c]=max_line_len
        self.coords_min_vals[c]=min_val
        if c % UPDATE_COUNT ==0:
            update_status('reading coordinates {}/{}'.format(c, ncoords))
        self.coord_counter=self.coord_counter+1
            
    def read_coordinates(self, verbose):
        if verbose:
            print('Database_reader.read_coordinates()')
        update_status('reading coordinates')
        rows=select_all_coords(self.cur)
        ncoords=len(rows)
        self.coords=[None]*ncoords # create list of appropriate size to hold coords
        self.coords_str=['']*ncoords
        self.coords_nlines=np.zeros(ncoords,int)
        self.coords_max_line_len=np.zeros(ncoords,int)
        self.coords_min_vals=[None]*ncoords
        self.coord_counter=0
        [self.create_coord(row, ncoords) for row in rows]
        update_status('')
        return self.coord_counter

    def read_files(self, did, filename_exp,verbose):
        if verbose:
            print('Database_reader.read_files() did=', did, filename_exp)
        update_status('reading files')
        self.files_metadata.read_from_database(self.cur,did,filename_exp)
        update_status('')
        return self.files_metadata.get_nfiles()

    def check_valid_variable(self, vix, fids, coord_filters):
        this_var=self.active_variables[vix]        
        # check if all coordinates and fids of this variable are in requested range
        coords_in_range, nactive_files=this_var.check_fids_and_filters(fids, coord_filters, self.coords)
        return coords_in_range, nactive_files

#----------------------------------------------------
# Directory button has been used to select directory
#----------------------------------------------------
def set_dirname(d):
    global current_db
    global unique_dirnames
    global databases
    global verbose
    if dirname_lab["text"]!=unique_dirnames[d]:
        if verbose:
            print('set_dirname(): setting dirname to', unique_dirnames[d])
        dirname_lab["text"]=unique_dirnames[d]
        results['state']='normal'
        results.delete("1.0",END)
        results['state']='disabled'
        # work out which database this dirname is in
        if unique_dirnames[d]=='*':
            current_db=-1
        else:
            for d in range(len(databases)):
                matches=databases[d].has_dirpath(unique_dirnames[d])
                if matches:
                    current_db=d
                    if verbose:
                        print('set_dirname(): current database is now', databases[current_db].dbname)
                    if len(databases[current_db].dirpaths)>1:
                        if databases[current_db].files_metadata.get_nfiles()>0:
                            # clear all files except those in selected directory
                            databases[current_db].files_metadata.clear()
    update_status('')
    
#----------------------------------------------------
# Filename has been set to filter files
#----------------------------------------------------
def set_filename(d):

    global current_db
    global databases

    results['state']='normal'
    results.delete("1.0",END)
    results['state']='disabled'

    if current_db==-1:
        for db in databases:
            if db.files_metadata.get_nfiles()>0:
                db.files_metadata.clear()
    else:
        if databases[current_db].files_metadata.get_nfiles()>0:
            databases[current_db].files_metadata.clear()
    update_status('')
    return True

#----------------------------------------------------
# Variable button has been used to select variable
#----------------------------------------------------
def set_variable(v):
    global current_db
    global unique_varnames
    global databases
    global verbose

    if variable_lab["text"]!=unique_varnames[v]:
        if verbose:
            print('set_variable(): setting variable to', unique_varnames[v])
        variable_lab["text"]=unique_varnames[v]
        results['state']='normal'
        results.delete("1.0",END)
        results['state']='disabled'
        # variable has changed so clear active_variables apart from the one we now want
        new_var=unique_varnames[v]
        if current_db==-1:
            for db in databases:
                this_nvars=len(db.active_variables)
                varnames=np.asarray([db.active_variables[i].name for i in range(this_nvars)])
                db.active_variables.clear()
        else:
            this_nvars=len(databases[current_db].active_variables)
            varnames=np.asarray([databases[current_db].active_variables[i].name for i in range(this_nvars)])
            databases[current_db].active_variables.clear()
    update_status('')
                
#-----------------------------------------------------
# validation of coord_filter entries
# %d = Type of action (1=insert, 0=delete, -1 for others)
# %P = value of the entry if the edit is allowed
# %s = value of entry prior to editing
# %S = the text string being inserted or deleted, if any
#-----------------------------------------------------
# this only allows positive or negative integers
#-----------------------------------------------------
def on_validate(d, P, s, S):

    ok=True
    if d=='1': ## insert
        if len(s)==0:
            ok=S in ['-','1','2','3','4','5','6','7','8','9'] # allow negative or positive number
        else:
            ok=S in ['0','1','2','3','4','5','6','7','8','9'] # allow a number
        if ok==True:
            results['state']='normal'
            results.delete("1.0",END)
            results['state']='disabled'
        
    return ok

#-----------------------------------------------------
# only allow a date in format YYYY-MM-DD
#-----------------------------------------------------
def on_validate_time(d, P, s, S):

    ok=True
    if d=='1': ## insert
        if len(s)==10:
            ok=False # got to end of date don't allow more
        else:
            if len(s)<4:
                # allow anything for the year
                ok=S in ['0','1','2','3','4','5','6','7','8','9']
            elif len(s)==4 or len(s)==7:
                # only allow '-'
                ok=S=='-'
            else:
                if len(s)==5:
                   # check 1st digit of month, i.e. 0 or 1 only
                   ok=S in ['0','1']
                elif len(s)==6:
                   # check 2nd digit of month i.e any digit from 1 if 1st digit of month is 0,
                   # or 0, 1 or 2 if 1st digit of month is 1
                   if s[-1]=='0':
                       ok=S in ['1','2','3','4','5','6','7','8','9']
                   else:
                       ok=S in ['0','1','2']
                elif len(s)==8:
                   # check 1st digit of day, i.e. 0,1,2 or 3 allowed
                   if s[5]=='0' and s[6]=='2':
                       # month is February
                       ok=S in ['0','1','2']
                   else:
                       ok=S in ['0','1','2','3']
                else:
                   # check last digit of day
                   if s[-1]=='0':
                       ok=S in ['1','2','3','4','5','6','7','8','9']
                   elif s[-1]!='3':
                       ok=S in ['0','1','2','3','4','5','6','7','8','9']
                   else:
                       ok=S in ['0','1']

            if ok==True:
                results['state']='normal'
                results.delete("1.0",END)
                results['state']='disabled'


    return ok
    
#-----------------------------------------------------
# Pop ups
#-----------------------------------------------------
#------------------------------------------------------------------------------
# show the attributes of a variable with index vix in database with index dbix 
#------------------------------------------------------------------------------
def popupVarDetails(event,var_tag,dbix,vix):
    # show the attributes of this variable
    global databases
    this_var=databases[dbix].active_variables[vix]
    attr_text,max_line_len=this_var.get_attributes_str()
    if len(attr_text)==0:
        attr_text='no attributes      ' # make some long enough text to see the title of the box
    info_window = Tk()
    info_window.title(this_var.name+': attributes')
    nlines=len(this_var.attributes)+1
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))

    label = Label(info_window, text=attr_text, anchor="w",justify='left',borderwidth=1, relief="solid", font=font)
    label.pack(fill=BOTH)

    info_window.mainloop()

#----------------------------------------------------------------------------------------
# show the coordinate details for coordinate with index cix in database with index dbix 
#----------------------------------------------------------------------------------------
def popupCoordDetails(event,coord_tag,dbix,cix):
    # show the range of this coordinate
    global databases
    coord_str=databases[dbix].coords_str[cix]
    info_window = Tk()
    info_window.title(databases[dbix].coords[cix].name)
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))
    label = Label(info_window, text=coord_str, anchor="w",justify='left',borderwidth=1, relief="solid", font=font)
    label.pack(fill=BOTH)

    info_window.mainloop()

#-------------------------------------------------------------------------------------------------------------------
# Show the details of all the coordinates of the dimension d of variable with index vix in database with index dbix
# which has multiple cids and fids.
# Also shows which file each coordinate is in
# We need to show them in order because there is no guarantee the coordinates will have
# been added to the database in order
#-------------------------------------------------------------------------------------------------------------------
def add_coord_filepath(text_str, dbix, this_coord_str, this_max_line_len, fid, c):
    global databases
    this_file=databases[dbix].files_metadata.get_matching_fid(fid)
    pathname=get_filepath(databases[dbix].dirpaths[this_file.did],this_file.filename)
    text_str[c]=this_coord_str+' in '+pathname+'\n'
    this_max_line_len=this_max_line_len+len(pathname)+len(' in ')
    if c % UPDATE_COUNT==0:
        update_status('getting coordinate info '+str(c)) # use c as counter as cids are not in numerical order necessarily
    return this_max_line_len

def popupMultiCoordDetails(event,tag,dbix,vix,d):
    global databases
    update_status('getting coordinate info')
    # show the ranges of the dimension of the variable that has multiple coordinates
    this_fixes=np.where(databases[dbix].active_variables[vix].allowed_fids==1)
    this_fids=databases[dbix].active_variables[vix].fids[this_fixes[0]]
    this_cids=np.asarray(databases[dbix].active_variables[vix].get_cids_for_dim(d))
    this_cids=this_cids[this_fixes[0]]
    assert(len(this_fids)!=0)
    coords_min_vals=np.asarray(databases[dbix].coords_min_vals)[this_cids]
    # sort  the coords
    ix=np.argsort(coords_min_vals)
    this_cids=this_cids[ix]
    this_fids=this_fids[ix]
    
    coords_str=np.asarray(databases[dbix].coords_str)[this_cids]
    coords_nlines=databases[dbix].coords_nlines[this_cids]
    coords_max_line_len=databases[dbix].coords_max_line_len[this_cids]
    # combine coord_str with filepath for that coord
    text_str=['']*len(this_cids)
    max_line_len=[add_coord_filepath(text_str, dbix,coords_str[c],coords_max_line_len[c],this_fids[c],c) for c in range(len(this_cids))]
    text_str=''.join(text_str)
    nlines=sum(coords_nlines)
    max_line_len=max(max_line_len)
    
    info_window = Tk()
    info_window.title(databases[dbix].active_variables[vix].name+': '+databases[dbix].coords[this_cids[0]].name)
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))

    max_height=12
    height=np.amin([max_height,nlines]) # show up to max_height lines as we have a scroll bar
    text = Text(info_window, borderwidth=1, width=max_line_len, height=height, relief="solid", font=font)
    text.insert(INSERT, text_str)

    ys = Scrollbar(info_window, orient = 'vertical', command = text.yview)
    text['yscrollcommand'] = ys.set
    ys.pack(side=RIGHT,fill=Y)
    text.pack(fill=BOTH)
    update_status('')

    info_window.mainloop()

#------------------------------------------------------------------------------------
# Show the global attributes of a file with given fid in database with index dbix
#------------------------------------------------------------------------------------
def popupFileAttributes(event,file_tag,dbix,fid):

    global databases
    this_file=databases[dbix].files_metadata.get_matching_fid(fid)
    dirpath=databases[dbix].dirpaths[this_file.did]
    filepath=get_filepath(dirpath, this_file.filename)
    file_str, max_line_len=this_file.get_file_attr_str(databases[dbix].cur)
    finfo_window = Tk()
    finfo_window.title(filepath+': global attributes')
    finfo_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))
    height=12
    width=min([100, max_line_len])
    text = Text(finfo_window, borderwidth=1, wrap="none", width=width, height=height, relief="solid", font=font)
    text.insert(INSERT, file_str)
    xs = Scrollbar(finfo_window, orient = 'horizontal', command = text.xview)
    text['xscrollcommand'] = xs.set
    xs.pack(side=BOTTOM,fill=X)
    text.pack(expand=1)

    finfo_window.mainloop()

#------------------------------------------------------------------------------------
# Show a list of all the valid files for variable with index vix in database with index dbix
# The variable will have allowed_fids stored.
# We need to show them in order which we determine from the values of the coordinates
# because there is no guarantee the files will have been added to the database in order
#------------------------------------------------------------------------------------
def popupFilesDetails(event,file_tag,dbix,vix):
    global databases
    update_status('getting file info')
    # we need the coord min_val for the multi dimension to be able to order the files even
    # though we wont display anything about the coord
    this_fixes=np.where(databases[dbix].active_variables[vix].allowed_fids==1)
    this_fids=np.asarray(databases[dbix].active_variables[vix].fids)
    this_fids=this_fids[this_fixes[0]]
    if len(this_fids)>1:
        # get which dimension has multiple files
        d=databases[dbix].active_variables[vix].get_multi_file_dimension()
        if d>=0:
            this_cids=np.asarray(databases[dbix].active_variables[vix].get_cids_for_dim(d))
            this_cids=this_cids[this_fixes[0]]
            # show the coords and files in ascending order by coord min_val
            min_vals=np.asarray(databases[dbix].coords_min_vals)[this_cids]
            ix=np.argsort(min_vals)
            this_fids=this_fids[ix]
        else:
            print(f'popupFilesDetails(): cannot find multi dimension for var {databases[dbix].active_variables[vix].name} to order fids, cids={databases[dbix].active_variables[vix].cids}')
            # we will just show them in alphabetical order
            files=[databases[dbix].files_metadata.get_matching_fid(fid) for fid in this_fids]
            filepaths=np.asarray([databases[dbix].dirpaths[this_file.did]+this_file.filename for this_file in files])
            ix=np.argsort(filepaths)
            this_fids=this_fids[ix]
            
    elif len(this_fids)==0:
        raise ValueError(f'popupFilesDetails(): no allowed fids for var {databases[dbix].active_variables[vix].name}')

    nlines=len(this_fids)
    max_line_len=80
    lines=['']*nlines  
        
    info_window = Tk()
    info_window.title(databases[dbix].active_variables[vix].name+': valid files')
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))
    max_height=12
    height=np.amin([max_height,nlines])
    text = Text(info_window, borderwidth=1, wrap="none", width=max_line_len, height=height, relief="solid", font=font)
    for l in range(nlines):
        fid=this_fids[l]
        if l % UPDATE_COUNT==0:
            update_status('getting file info '+str(l)) # use l as counter as fids are not in numerical order neccessarily
        this_file=databases[dbix].files_metadata.get_matching_fid(fid)
        lines[l]=this_file.get_file_info_str(databases[dbix].dirpaths[this_file.did])
        file_tag='file_tag{t:d}'.format(t=this_fids[l])
        text.insert(INSERT, lines[l],(file_tag))
        text.tag_bind(file_tag, '<Button-1>', lambda e,dbix=dbix,fid=this_fids[l]:popupFileAttributes(e,file_tag,dbix,fid))

    ys = Scrollbar(info_window, orient = 'vertical', command = text.yview)
    text['yscrollcommand'] = ys.set
    ys.pack(side=RIGHT,fill=Y)
    xs = Scrollbar(info_window, orient = 'horizontal', command = text.xview)
    text['xscrollcommand'] = xs.set
    xs.pack(side=BOTTOM,fill=X)
    text.pack(expand=1)
    update_status('')
    
    info_window.mainloop()


#-------------------------------------------------------------------------------
# display the variable details and create popups to display more details
# inputs:
#    dbix - the index into databases
#    fids - the valid fids ([] is all valid)
#    vix - index into the active_variables of the database
#    ftag, vtag and ctag are numbers used to form a unique tag for the popup
# returns:
#    valid, ftag, vtag, ctag - updated
#-------------------------------------------------------------------------------
def show_valid_variable(dbix, fids, vix, ftag, vtag, ctag):

    global databases
    global verbose

    # check if all coordinates and fids of this variable are in requested range
    coords_in_range, nactive_files=databases[dbix].check_valid_variable(vix, fids, coord_filters)
    if verbose:
        print('show_valid_variable(): ', databases[dbix].active_variables[vix].name, nactive_files,'active_files')
    if coords_in_range and nactive_files>0:
        this_var=databases[dbix].active_variables[vix]
        var_tag='var_attr{t:d}'.format(t=vtag)
        vtag=vtag+1
        results.insert(INSERT, this_var.name+' (',(var_tag))
        results.tag_bind(var_tag, '<Button-1>', lambda e,dbix=dbix,vix=vix:popupVarDetails(e,var_tag,dbix,vix))
        for d in range(this_var.ndims):
            this_cids=this_var.get_cids_for_dim(d)
            dimname=databases[dbix].coords[this_cids[0]].name
            coord_tag='coord_tag{t:d}'.format(t=ctag)
            ctag=ctag+1
            if len(this_cids)==1:
                # can have a popup for coord details
                results.insert(INSERT, dimname+',',(coord_tag))
                results.tag_bind(coord_tag, '<Button-1>', lambda e,dbix=dbix,cix=this_cids[0]:popupCoordDetails(e,coord_tag,dbix,cix))
            else:
                # more than one coord covers the range
                results.insert(INSERT, dimname+',',(coord_tag))
                results.tag_bind(coord_tag, '<Button-1>', lambda e,dbix=dbix,vix=vix,d=d:popupMultiCoordDetails(e,coord_tag,dbix,vix,d))
        files_tag='files_details{t:d}'.format(t=ftag)
        ftag=ftag+1
        results.insert(INSERT, ') for {n:d} files\n'.format(n=nactive_files),files_tag)
        results.tag_bind(files_tag, '<Button-1>', lambda e,dbix=dbix,vix=vix:popupFilesDetails(e,files_tag,dbix,vix))

    return (coords_in_range and nactive_files>0), ftag, vtag, ctag

#--------------------------------------------------------------------------------
# Search button pressed
# read all the filters (dirname, variable and coord_filters)
# select the appropriate variables and which fids of those variables are allowed
# display the variable details and create popups to display more details
#-------------------------------------------------------------------------------
def search_db():
    global databases
    global current_db
    global verbose

    results['state']='normal'
    results.delete("1.0",END)        
    if verbose:
        print('search_db():',dirname_lab["text"]+'/*'+filename_entry.get(),'variable=',variable_lab["text"])

    dirname=dirname_lab["text"]
    filename_exp=filename_entry.get()
    variable=variable_lab["text"]
    # if we have not changed the variable since last search db.active_variables will still have the variables in it
    nvars=0
    nfiles=0
    nvars_valid=0
    # set up the coord_filters min max values from the widgets
    for i in range(nfilters):
        if coord_filters[i].is_valid:
            coord_filters[i].get()

    ftag=0
    vtag=0
    ctag=0
    if dirname=='*':
        for dbix in range(len(databases)):
            db=databases[dbix]
            this_nfiles=db.read_files(-1, filename_exp, verbose)
            if verbose:
                print('search_db(): found', this_nfiles, 'files in database', db.dbname)
            nfiles=nfiles+this_nfiles
            if this_nfiles>0:
                fids=databases[current_db].files_metadata.get_fids()
                this_ncoords=len(db.coords)
                if this_ncoords==0:
                    this_ncoords=db.read_coordinates(verbose)
                this_nvars=len(db.active_variables)
                if this_nvars==0:
                    this_nvars=db.read_variables(variable,verbose)
                update_status('checking which variables are valid')
                for vix in range(this_nvars):
                    is_valid, ftag, vtag, ctag=show_valid_variable(dbix, fids, vix, ftag, vtag, ctag)
                    if is_valid:
                        nvars_valid+=1
                update_status('')
                nvars=nvars+this_nvars

    else:
        did=databases[current_db].get_did(dirname)
        # get files with matching did
        nfiles=databases[current_db].read_files(did, filename_exp,verbose)
        if verbose:
            print('search_db(): found', nfiles, 'files in database', databases[current_db].dbname, 'with did', did, dirname)
        if nfiles>0:
            fids=databases[current_db].files_metadata.get_fids()
            this_ncoords=len(databases[current_db].coords)
            if this_ncoords==0:
                this_ncoords=databases[current_db].read_coordinates(verbose)
            nvars=len(databases[current_db].active_variables)
            if nvars==0:
                nvars=databases[current_db].read_variables(variable,verbose)
            if verbose:
                print('search_db(): read', nvars, 'variables')
            update_status('checking which variables are valid')
            for vix in range(nvars):
                is_valid, ftag, vtag, ctag=show_valid_variable(current_db, fids, vix, ftag, vtag, ctag)
                if is_valid:
                    nvars_valid+=1

    update_status('Found {} files, {} variables in database ({} valid)'.format(nfiles, nvars, nvars_valid))

    results['state']='disabled'

##############################################################################################
# start of main code
##############################################################################################
#-----------------------------------------------------------------
# read in the arguments, open the database and display the screen
#-----------------------------------------------------------------
if len(sys.argv)<2:
    print('usage: ', sys.argv[0], '<dbname>', '<[coord1 coord2...]> <-v>')
    exit()


dbname_or_dir=sys.argv[1]
for i in range(2,len(sys.argv)):
    if sys.argv[i]=='-v':
        verbose=True
    else:
        coord_filters.append(Coord_filter(sys.argv[i]))
nfilters=len(coord_filters)

# read the database(s)
wsplit=dbname_or_dir.split('.')
if wsplit[-1]=='db':
    databases.append(Database_reader(dbname_or_dir,verbose))
    unique_dirnames=unique_dirnames+databases[-1].dirpaths
    unique_varnames=unique_varnames+databases[-1].unique_varnames
else:
    for dirpath, dirnames, filenames in os.walk(dbname_or_dir):
        for filename in filenames:
            wsplit=filename.split('.')
            if wsplit[-1]=='db':
                databases.append(Database_reader(dirpath+'/'+filename,verbose))
                unique_dirnames=unique_dirnames+databases[-1].dirpaths
                unique_varnames=unique_varnames+databases[-1].unique_varnames

if len(unique_dirnames)>1:
    unique_dirnames=['*']+unique_dirnames
unique_varnames=['*']+list(np.unique(np.asarray(unique_varnames)))
if verbose:
    print(len(unique_varnames)-1, 'unique varnames')
    print(unique_varnames)

#-------------------------------------------------------------------------------------
# create the root window and set up frames to display widgets:
# setup_frame will contain all the widgets used to select what you want to search for
# results_frame will contain all the widgets to display the search results
# status_frame contains a widget to display what is going on when we have long database operations
#-------------------------------------------------------------------------------------
root = Tk()
root.title('metaview: '+dbname_or_dir)
root.resizable(False, False) # don't let user resize

setup_frame = Frame(master=root,relief=RIDGE, borderwidth=5)
setup_frame.pack(fill=BOTH)
results_frame = Frame(master=root,relief=RIDGE, borderwidth=5)
results_frame.pack(fill=BOTH)
status_frame = Frame(master=root,relief=RIDGE, borderwidth=5)
status_frame.pack(fill=BOTH)
# status bar
status_bar = Label(master=status_frame, width=100, font=font, fg='black', bg='white',anchor='w',borderwidth=3, relief="ridge")
status_bar.grid(row=0, column=0, sticky='W', pady=2)

#-------------------------------------------------------------------------------------
# the setup frame allows you to chose various options to filter your results
# you can select a single directory or all directories
# you can select a single variable or all variables
# you can specify coordinate ranges
#-------------------------------------------------------------------------------------

# selecting dirname
dirname_mb = Menubutton(setup_frame, text ="Directory", relief=RAISED, width=10, font=font)
dirname_mb.menu = Menu ( dirname_mb, tearoff = 0 )
dirname_mb["menu"] = dirname_mb.menu
for d in range(len(unique_dirnames)):
    dirname_mb.menu.add_command(label=unique_dirnames[d], command=lambda d=d: set_dirname(d), font=font)
dirname_mb.grid(row=0,column=0, sticky='W', pady=2)
# the label to show what has been chosen
dirname_lab = Label(master=setup_frame, text=unique_dirnames[0],width=70,borderwidth=1, anchor='w', relief="solid", font=font)
dirname_lab.grid(row=0, column=1, sticky='W', pady=2, columnspan=5)

# selecting variable
variable_mb = Menubutton(setup_frame, text ="Variable",relief=RAISED, width=10, font=font)
variable_mb.menu = Menu ( variable_mb, tearoff = 0 )
variable_mb["menu"] = variable_mb.menu
for v in range(len(unique_varnames)):
    variable_mb.menu.add_command(label=unique_varnames[v], command=lambda v=v: set_variable(v), font=font)
#vs = Scrollbar(variable_mb, orient = 'vertical', command = variable_mb.menu.yview)
#variable_mb.menu['yscrollcommand'] = vs.set
#vs.pack(side=RIGHT,fill=Y)
variable_mb.grid(row=1,column=0, sticky='W', pady=2)
# the label to show what variable has been chosen
variable_lab = Label(master=setup_frame, text=unique_varnames[0],width=70,borderwidth=1,anchor='w', relief="solid", font=font)
variable_lab.grid(row=1, column=1, sticky='W', pady=2, columnspan=5)

# selecting filename - free form so handles regular expressions
vcmd_filename = (setup_frame.register(set_filename), '%d')
filename_lab = Label(master=setup_frame, text='Filename:', anchor='w', font=font)
filename_lab.grid(row=2, column=0, sticky='W', pady=2)
filename_entry = Entry(master=setup_frame, width=10, font=font, validate="key", validatecommand=vcmd_filename)
filename_entry.grid(row=2, column=1, sticky='W', pady=2)
#filename_entry.insert(0,'*')


# set up widgets to handle bespoke filtering on coordinate values
vcmd_number = (setup_frame.register(on_validate),
                '%d', '%P', '%s', '%S')
vcmd_time = (setup_frame.register(on_validate_time),
                '%d', '%P', '%s', '%S')
row=4
for i in range(nfilters):
    width=5
    txt_extra=''
    vcmd=vcmd_number
    first_db=True
    for db in databases:
        is_time=db.coordinate_filter_is_time(coord_filters[i])
        if first_db:
            if is_time:
                coord_filters[i].is_time=True   
                vcmd=vcmd_time
                txt_extra=' (YYYY-MM-DD)'
                width=10
        elif coord_filters[i].is_time!=is_time:
            raise ValueError(f'coordinates in different database matching {coord_filters[i].name} are time and not time!')
        first_db=False
    if coord_filters[i].is_valid:
        coord_min_txt = Label(master=setup_frame, text='min '+coord_filters[i].name+txt_extra+':', anchor='w', font=font)
        coord_min_txt.grid(row=row, column=0, sticky='W', pady=2)
        coord_filters[i].min_widget = Entry(master=setup_frame, width=width, validate="key", validatecommand=vcmd, font=font)
        coord_filters[i].min_widget.grid(row=row, column=1, sticky='W', pady=2)
        coord_max_txt = Label(master=setup_frame, text='max '+coord_filters[i].name+txt_extra+':', anchor='w', font=font)
        coord_max_txt.grid(row=row, column=2,stick='W', pady=2)
        coord_filters[i].max_widget = Entry(master=setup_frame, width=width, validate="key", validatecommand=vcmd, font=font)
        coord_filters[i].max_widget.grid(row=row, column=3, sticky='W',pady=2)
        row=row+1
    
# search button to kick off search
searchB = Button(setup_frame, text ="Search", command = search_db, font=font)
searchB.grid(row=row+1,column=5, sticky='W',pady=2)

# widget to display results
results = Text(results_frame, state='disabled', height=20, width=100, font=font)
ys = Scrollbar(results_frame, orient = 'vertical', command = results.yview)
results['yscrollcommand'] = ys.set
ys.pack(side=RIGHT,fill=Y)
results.pack()


# kick it all off
setup_frame.pack()
results_frame.pack()
status_frame.pack(side="left")

root.mainloop()

