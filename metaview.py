
'''
    Code to display a GUI to explore the database created by build_metdata_db.py

    Usage: python metaview.py <dbname> [coord1 coord2...] -v

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

#----------------------------------------------------
# Directory button has been used to select directory
#----------------------------------------------------
def set_dirname(d):
    if dirname_lab["text"]!=unique_dirnames[d]:
        print('setting dirname to', unique_dirnames[d])
        dirname_lab["text"]=unique_dirnames[d]
        results['state']='normal'
        results.delete("1.0",END)
        results['state']='disabled'
        if files_metadata.get_nfiles()>0:
            files_metadata.clear()

#----------------------------------------------------
# Filename button has been used to select files
#----------------------------------------------------
def set_filename(d):

    results['state']='normal'
    results.delete("1.0",END)
    results['state']='disabled'
    if files_metadata.get_nfiles()>0:
        files_metadata.clear()
    return True

#----------------------------------------------------
# Variable button has been used to select variable
#----------------------------------------------------
def set_variable(v):
    if variable_lab["text"]!=unique_varnames[v]:
        print('setting variable to', unique_varnames[v])
        variable_lab["text"]=unique_varnames[v]
        results['state']='normal'
        results.delete("1.0",END)
        results['state']='disabled'
        # variable has changed so clear active_variables
        active_variables.clear()

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
#-----------------------------------------------------
# show the attributes of a variable with index vix
#-----------------------------------------------------
def popupVarDetails(event,var_tag,vix):
    # show the attributes of this variable
    this_var=active_variables[vix]
    attr_text,max_line_len=this_var.get_attributes_str()
    info_window = Tk()
    info_window.title(this_var.name+': attributes')
    nlines=len(this_var.attributes)+1
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))

    label = Label(info_window, text=attr_text, anchor="w",justify='left',borderwidth=1, relief="solid", font=font)
    label.pack(fill=BOTH)

    info_window.mainloop()

#-----------------------------------------------------
# show the coordinate details for coordinate with index cix
#-----------------------------------------------------
def popupCoordDetails(event,coord_tag,cix):
    # show the range of this coordinate
    this_coord=coords[cix]
    coord_str,nlines,max_line_len=this_coord.get_min_max_delta_str()
    info_window = Tk()
    info_window.title(this_coord.name)
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))
    label = Label(info_window, text=coord_str, anchor="w",justify='left',borderwidth=1, relief="solid", font=font)
    label.pack(fill=BOTH)

    info_window.mainloop()

#------------------------------------------------------------------------------------
# Show the details of all the coordinates of the dimension of variable with index vix
# which has multiple cids and fids.
# Also shows which file each coordinate is in
# We need to show them in order because there is no guarantee the coordinates will have
# been added to the database in order
#------------------------------------------------------------------------------------
def popupMultiCoordDetails(event,tag,vix,d):
    # show the ranges of the dimension of the variable that has multiple coordinates
    nlines=0
    lines=[]
    max_line_len=0
    # get which dimension has multiple files
    this_fixes=np.where(active_variables[vix].allowed_fids==1)
    this_fids=active_variables[vix].fids[this_fixes[0]]
    this_cids=np.asarray(active_variables[vix].cids[d])
    this_cids=this_cids[this_fixes[0]]
    if len(this_fids)>1:
        # show the coords and files in ascending order by coord min_val
        min_vals=np.asarray([coords[cix].get_min_max_delta()[0] for cix in this_cids])
        ix=np.argsort(min_vals)
        this_cids=this_cids[ix]
        this_fids=this_fids[ix]

    for c in range(len(this_cids)):
        cix=this_cids[c]
        coord_str, this_nlines,this_max_line_len=coords[cix].get_min_max_delta_str()
        fid=this_fids[c]
        this_file=files_metadata.get_matching_fid(fid)
        pathname=get_filepath(all_dirpaths[this_file.did],this_file.filename)
        text_str=coord_str+' in '+pathname+'\n'
        this_max_line_len=this_max_line_len+len(pathname)+1
        nlines=nlines+this_nlines
        lines.append(text_str)
        max_line_len=np.amax([max_line_len,this_max_line_len])
    info_window = Tk()
    info_window.title(active_variables[vix].name+': '+coords[this_cids[0]].name)
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))

    max_height=12
    height=np.amin([max_height,nlines]) # show up to max_height lines as we have a scroll bar
    text = Text(info_window, borderwidth=1, width=max_line_len, height=height, relief="solid", font=font)
    for l in range(nlines):
        text.insert(INSERT, lines[l])

    ys = Scrollbar(info_window, orient = 'vertical', command = text.yview)
    text['yscrollcommand'] = ys.set
    ys.pack(side=RIGHT,fill=Y)
    text.pack(fill=BOTH)

    info_window.mainloop()

#------------------------------------------------------------------------------------
# Show the details of a file with given fid
#------------------------------------------------------------------------------------
def popupFileDetails(event,file_tag,fid):

    this_file=files_metadata.get_matching_fid(fid)
    dirpath=all_dirpaths[this_file.did]

    file_str, nlines,max_line_len=this_file.get_file_info_str(dirpath)
    info_window = Tk()
    info_window.overrideredirect(1)
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))

    label = Label(info_window, text=file_str, anchor="w",justify='left',borderwidth=1, relief="solid", font=font)
    label.pack(fill=BOTH)

    info_window.bind_all("<Button-1>", lambda e: info_window.destroy())  # Remove popup when user clicks in the window
    info_window.mainloop()

#------------------------------------------------------------------------------------
# Show a list of all the valid files for variable with index vix
# The variable will have allowed_fids stored.
# We need to show them in order which we determine from the values of the coordinates
# because there is no guarantee the files will have been added to the database in order
#------------------------------------------------------------------------------------
def popupFilesDetails(event,file_tag,vix):
    nlines=0
    lines=[]
    max_line_len=0
    # we need to get the coord min and max to be able to order the files even though we wont display anything
    # about the coord
    this_fixes=np.where(active_variables[vix].allowed_fids==1)
    this_fids=np.asarray(active_variables[vix].fids)
    this_fids=this_fids[this_fixes[0]]
    if len(this_fids)>0:
        # get which dimension has multiple files
        d=active_variables[vix].get_multi_file_dimension()
        if d>=0:
            this_cids=np.asarray(active_variables[vix].cids[d])
            this_cids=this_cids[this_fixes[0]]
            # show the coords and files in ascending order by coord min_val
            min_vals=np.asarray([coords[cix].get_min_max_delta()[0] for cix in this_cids])
            ix=np.argsort(min_vals)
            this_fids=this_fids[ix]
        else:
            print('cannot find multi dimension to order fids')
            pdb.set_trace()

    for f in range(len(this_fids)):
        fid=this_fids[f]
        this_file=files_metadata.get_matching_fid(fid)
        pathname=get_filepath(all_dirpaths[this_file.did], this_file.filename)
        text_str=pathname+'\n'
        this_max_line_len=len(pathname)
        nlines=nlines+1
        lines.append(text_str)
        max_line_len=np.amax([max_line_len,this_max_line_len])

    info_window = Tk()
    info_window.title(active_variables[vix].name+': valid files')
    info_window.geometry("+{0}+{1}".format(event.x_root+6, event.y_root+2))

    max_height=12
    height=np.amin([max_height,nlines])
    text = Text(info_window, borderwidth=1, width=max_line_len, height=height, relief="solid", font=font)
    for l in range(nlines):
        file_tag='file_tag{t:d}'.format(t=this_fids[l])
        text.insert(INSERT, lines[l],(file_tag))
        text.tag_bind(file_tag, '<Button-1>', lambda e,fid=this_fids[l]:popupFileDetails(e,file_tag,fid))


    ys = Scrollbar(info_window, orient = 'vertical', command = text.yview)
    text['yscrollcommand'] = ys.set
    ys.pack(side=RIGHT,fill=Y)
    text.pack(fill=X)

    info_window.mainloop()


#--------------------------------------------------------------------------------
# Search button pressed
# read all the filters (dirname, variable and coord_filters)
# select the appropriate variables and which fids of those variables are allowed
# display the variable details and create popups to display more details
#-------------------------------------------------------------------------------
def search_db():
    results['state']='normal'
    results.delete("1.0",END)        
    print('searching',dirname_lab["text"]+'/*'+filename_entry.get(),'variable=',variable_lab["text"])

    dirname=dirname_lab["text"]
    filename_exp=filename_entry.get()

    if dirname=='*':
        files_metadata.read_from_database(cur,-1,filename_exp)
        fids=[]  # allow all
        nfiles=files_metdata.get_nfiles()
    else:
        dix=np.where(np.asarray(all_dirpaths)==dirname)
        # get files with matching did
        did=int(dix[0][0])
        files_metadata.read_from_database(cur, did, filename_exp)
        fids=files_metadata.get_fids()
        nfiles=len(fids)
        print(nfiles, 'fids')

    # set up the coord_filters min max values from the widgets
    for i in range(nfilters):
        coord_filters[i].get()
            
    variable=variable_lab["text"]
    # if we have not changed the variable since last search active_Variables will still have the variables in it
    nvars=0
    if nfiles>0:
        nvars=len(active_variables) 
    if nvars==0 and nfiles>0:
        if variable=='*':
            # we are looking for all variables
            var_rows=select_all_variables(cur)
        else:
            # we are looking for a specific variable
            var_rows=select_variables_by_name(variable,cur)
            
        for row in var_rows:
            this_var=Variable_metadata(row, cur)
            active_variables.append(this_var)
            nvars=nvars+1

    ctag=0
    for vix in range(nvars):
        this_var=active_variables[vix]        
        # check if all coordinates and fids of this variable are in requested range
        coords_in_range, nactive_files=this_var.check_fids_and_filters(fids, coord_filters, coords)

        if coords_in_range and nactive_files>0:
            var_tag='var_attr{t:d}'.format(t=vix)
            results.insert(INSERT, this_var.name+' (',(var_tag))
            results.tag_bind(var_tag, '<Button-1>', lambda e,vix=vix:popupVarDetails(e,var_tag,vix))
            for d in range(this_var.ndims):
                dimname=coords[this_var.cids[d][0]].name
                coord_tag='coord_tag{t:d}'.format(t=ctag)
                ctag=ctag+1
                if len(this_var.cids[d])==1:
                    # can have a popup for coord details
                    results.insert(INSERT, dimname+',',(coord_tag))
                    results.tag_bind(coord_tag, '<Button-1>', lambda e,cix=this_var.cids[d][0]:popupCoordDetails(e,coord_tag,cix))
                else:
                    # more than one coord covers the range
                    results.insert(INSERT, dimname+',',(coord_tag))
                    results.tag_bind(coord_tag, '<Button-1>', lambda e,vix=vix,d=d:popupMultiCoordDetails(e,coord_tag,vix,d))
            files_tag='files_details{t:d}'.format(t=vix)
            results.insert(INSERT, ') for {n:d} files\n'.format(n=nactive_files),files_tag)
            results.tag_bind(files_tag, '<Button-1>', lambda e,vix=vix:popupFilesDetails(e,files_tag,vix))


    results['state']='disabled'

#-----------------------------------------------------------------
# read in the arguments, open the database and display the screen
#-----------------------------------------------------------------
if len(sys.argv)<2:
    print('usage: ', sys.argv[0], '<dbname>', '<[coord1 coord2...]> <-v>')
    exit()

dbname=sys.argv[1]
verbose=False
coord_filters=[]
for i in range(2,len(sys.argv)):
    if sys.argv[i]=='-v':
        verbose=True
    else:
        coord_filters.append(Coord_filter(sys.argv[i]))
nfilters=len(coord_filters)

# open the database
if os.path.isfile(dbname)==False:
    print('No such database', dbname)
    exit()

if verbose:
    print('opening database', dbname)
con = sqlite3.connect(dbname)
cur = con.cursor()

#-----------------------------------------------------------------------------------
# get a list of all directory names from database
#-----------------------------------------------------------------------------------
all_dirpaths=read_all_directories(cur)
if len(all_dirpaths)==1:
    unique_dirnames=all_dirpaths
else:
    unique_dirnames=['*']
    unique_dirnames=unique_dirnames+all_dirpaths

#-----------------------------------------------------------------------------------
# get list of all unique variable names from database - we only need name at this stage
#-----------------------------------------------------------------------------------
unique_varnames=['*']
res=cur.execute("""SELECT name FROM Variables""")
all_varnames=np.asarray(res.fetchall())
if verbose:
    print(len(all_varnames), 'variables', len(np.unique(all_varnames)), 'unique')
# just get the unique varnames
for v in np.unique(all_varnames):
    unique_varnames.append(v)

#-----------------------------------------------------------------------------------
# read all the coords but have a place to store variables and files that have been searched for
#-----------------------------------------------------------------------------------
coords=[]

for row in select_all_coords(cur):
    coords.append(Coord_metadata(row,cur))
    #if verbose:
    #    coords[-1].print()
ncoords=len(coords)
if verbose:
    print(ncoords, 'Coords') 

# place to store the files- initially no files but read on a search        
files_metadata=Files_metadata()
# hold the variables that were searched for here - initially empty
active_variables=[]


#-------------------------------------------------------------------------------------
# create the root window and set up frames to display widgets:
# setup_frame will contain all the widgets used to select what you want to search for
# results_frame will contain all the widgets to display the search results
#-------------------------------------------------------------------------------------
root = Tk()
root.title('metaview: '+dbname)
root.resizable(False, False) # don't let user resize

setup_frame = Frame(master=root,relief=RIDGE, borderwidth=5)
setup_frame.pack(fill=BOTH)
results_frame = Frame(master=root,relief=RIDGE, borderwidth=5)
results_frame.pack(fill=BOTH)

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
coord_names=np.asarray([coord.name for coord in coords])
for i in range(nfilters):
    width=5
    txt_extra=''
    vcmd=vcmd_number
    ix=np.where(coord_names==coord_filters[i].name)
    if len(ix[0])>0:
        if coords[ix[0][0]].is_time():
            coord_filters[i].is_time=True
            vcmd=vcmd_time
            txt_extra=' (YYYY-MM-DD)'
            width=10

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

# widgets to display results
results = Text(results_frame, state='disabled', height=20, width=100, font=font)
ys = Scrollbar(results_frame, orient = 'vertical', command = results.yview)
results['yscrollcommand'] = ys.set
ys.pack(side=RIGHT,fill=Y)
results.pack()

# kick it all off
setup_frame.pack()
results_frame.pack()

root.mainloop()

