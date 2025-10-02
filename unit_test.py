#--------------------------------------------------------------
# used to test function in db_functions.py for inserting coords
# and variables into the database and retrieving them again
# We can test out different types of coords and variables
# to aid in bug fixing
#---------------------------------------------------------------
import sys
import os
from db_functions import *
from read_metadata_thread import *
import numpy as np
import sqlite3
import pdb


def test_coord_creation(thr):

    print('--------------------------\nCreating coordinates\n--------------------------')
    # create a coordinate with no data
    # time coordinate with calendar, uniform times
    times=np.asarray([0,24,48])
    t_units='hours since 2000-01-01'
    coord_time=Coord_metadata(UNKNOWN_ID, 'time', times, thr.thread_name)
    coord_time.add_attribute('units',t_units)
    coord_time.add_attribute('calendar','julian')
    thr.create_or_find_matching_coord(coord_time)
    assert(len(thr.coords)==1)
    assert(thr.coords[0].cid==0)
    thr.coords[0].print()
    is_time, calendar=thr.coords[0].is_time()
    assert(is_time)
    print('coord_time passed')
    
    # time coordinate with calendar, uniform times but different times
    coord_time2=Coord_metadata(UNKNOWN_ID, 'time', times+24, thr.thread_name)
    coord_time2.add_attribute('units',t_units)
    coord_time2.add_attribute('calendar','julian')
    thr.create_or_find_matching_coord(coord_time2)
    assert(len(thr.coords)==2)
    assert(thr.coords[1].cid==1)
    thr.coords[1].print()
    is_time, calendar=thr.coords[1].is_time()
    assert(is_time)
    print('coord_time2 passed')
   
    # time coordinate no calendar, uniform times
    coord_time_no_calendar=Coord_metadata(UNKNOWN_ID, 'time', times, thr.thread_name)
    coord_time_no_calendar.add_attribute('units',t_units)
    thr.create_or_find_matching_coord(coord_time_no_calendar)
    assert(len(thr.coords)==3)
    assert(thr.coords[2].cid==2)
    thr.coords[2].print()
    is_time, calendar=thr.coords[2].is_time()
    assert(is_time)
    print('coord_time_no_calendar passed')
    
    # time coordinate, non-uniform times
    times2=np.asarray([0,24,40,36])
    coord_time_non_uniform=Coord_metadata(UNKNOWN_ID, 'time', times2, thr.thread_name)
    coord_time_non_uniform.add_attribute('units',t_units)
    coord_time_non_uniform.add_attribute('calendar', 'gregorian')
    thr.create_or_find_matching_coord(coord_time_non_uniform)
    assert(len(thr.coords)==4)
    assert(thr.coords[3].cid==3)
    thr.coords[3].print()
    is_time, calendar=thr.coords[3].is_time()
    assert(is_time)
    print('coord_time_non_uniform passed')
    
    # coordinate with masked data
    data=np.asarray([1.0,2.0,3.0,4.0])
    masked_data= np.ma.masked_array(data, mask=[0, 0, 1, 0])
    coord_masked=Coord_metadata(UNKNOWN_ID, 'masked_data', masked_data, thr.thread_name)
    thr.create_or_find_matching_coord(coord_masked)
    assert(len(thr.coords)==5)
    assert(thr.coords[4].cid==4)
    thr.coords[4].print()
    is_time, calendar=thr.coords[4].is_time()
    assert(is_time==False)
    print('coord_masked passed')

    # coordinate with integer masked data
    data=np.arange(4)+3
    masked_data= np.ma.masked_array(data, mask=[0, 0, 1, 0])
    coord_masked2=Coord_metadata(UNKNOWN_ID, 'masked_data', masked_data, thr.thread_name)
    thr.create_or_find_matching_coord(coord_masked2)
    assert(len(thr.coords)==6)
    assert(thr.coords[5].cid==5)
    thr.coords[5].print()
    print('coord_masked int passed')
    
    # dimension with no data
    no_values=[]
    coord_no_data=Coord_metadata(UNKNOWN_ID, 'coord_no_data', no_values, thr.thread_name)
    coord_no_data.add_attribute('dimension_attr','no values in this dimension')
    thr.create_or_find_matching_coord(coord_no_data)
    assert(len(thr.coords)==7)
    assert(thr.coords[6].cid==6)
    thr.coords[6].print()
    print('coord_no_data passed')
    
        
def test_coords_from_database(thr):
    res=select_all_coords(thr.cur)
    assert(len(res)==len(thr.coords))
    cid=0
    print('--------------------------\nReading coordinates from database\n--------------------------')
    for row in res:
        coord=Coord_metadata(row, thr.cur)
        (this_str, nlines, max_line_len, min_val)=coord.get_min_max_delta_str()
        assert(coord.cid==thr.coords[cid].cid)
        assert(coord.name==thr.coords[cid].name)
        assert(coord.nvals==thr.coords[cid].nvals)
        if coord.nvals>0:
            assert(coord.min_val==thr.coords[cid].min_val)
            assert(coord.max_val==thr.coords[cid].max_val)
            assert(coord.delta==thr.coords[cid].delta)
            assert(len(coord.values)==len(thr.coords[cid].values))
        else:
            print('min_val', coord.min_val, thr.coords[cid].min_val)
        if len(coord.values)>0:
            assert(np.any(abs(np.asarray(coord.values)-np.asarray(thr.coords[cid].values))<1e-6))
        print('coord', coord.cid, coord.name, this_str, coord.values, 'matches created')
        cid+=1
        
def test_variable_creation(thr):

    print('--------------------------\nCreating variables\n--------------------------')

    nvars=0
    # create var with single dimension
    this_var=Variable_metadata(UNKNOWN_ID,'one_d',1)
    this_var.attributes=[Attribute('long_name','one_dimensional_variable')]
    cids=[0] # a time dimension
    this_var.add_cids_for_fid(0, cids)
    thr.create_or_find_matching_variable(this_var)
    nvars+=1
    assert(len(thr.variables)==nvars)
    assert(thr.variables[nvars-1].vid==nvars-1)
    thr.variables[nvars-1].print()
    print('one_d variable passed')
    
    # create a 2d variable
    this_var=Variable_metadata(UNKNOWN_ID,'two_d',2)
    this_var.attributes=[Attribute('long_name','two_dimensional_variable')]
    cids=[0,4] # a time for first dimension and masked data coord for 2nd dimension
    this_var.add_cids_for_fid(0, cids)
    thr.create_or_find_matching_variable(this_var)
    nvars+=1
    assert(len(thr.variables)==nvars)
    assert(thr.variables[nvars-1].vid==nvars-1)
    thr.variables[nvars-1].print()
    print('two_d variable passed')

    # a matching 2d variable with a different fid and time dim
    this_var=Variable_metadata(UNKNOWN_ID,'two_d',2)
    this_var.attributes=[Attribute('long_name','two_dimensional_variable')]
    cids=[1,4] # a time dimension of same units and calendar but different times for first dimension and masked data coord for 2nd dimension
    this_var.add_cids_for_fid(1, cids)
    thr.create_or_find_matching_variable(this_var) # this should match the variable to the one above
    assert(len(thr.variables)==nvars) # no new variable created
    thr.variables[nvars-1].print()
    print('matches previous variable')
    assert(len(thr.variables[nvars-1].fids)==2)
    assert(thr.variables[nvars-1].fids[0]==0)
    assert(thr.variables[nvars-1].fids[1]==1)
    assert(len(thr.variables[nvars-1].cids)==2)
    assert(len(thr.variables[nvars-1].cids[0])==2)
    assert(thr.variables[nvars-1].cids[0][0]==0)
    assert(thr.variables[nvars-1].cids[0][1]==1)
    assert(len(thr.variables[nvars-1].cids[1])==2)
    assert(thr.variables[nvars-1].cids[1][0]==4)
    print('two_d variable passed')
    
    # another 2d variable that looks much the same but has a different type of time coord 
    this_var=Variable_metadata(UNKNOWN_ID,'two_d',2)
    this_var.attributes=[Attribute('long_name','two_dimensional_variable')]
    cids=[2,4] # a time dimension of same units and calendar but different times for first dimension and masked data coord for 2nd dimension
    this_var.add_cids_for_fid(2, cids)
    thr.create_or_find_matching_variable(this_var) # this should match the variable to the one above
    nvars+=1
    assert(len(thr.variables)==nvars)
    assert(thr.variables[nvars-1].vid==nvars-1)
    thr.variables[nvars-1].print()
    print('another two_d variable passed')

    # another 2d variable that looks much the same as the first but has a different time coord and 2nd dimension coord
    this_var=Variable_metadata(UNKNOWN_ID,'two_d',2)
    this_var.attributes=[Attribute('long_name','two_dimensional_variable')]
    cids=[1,5] # a time dimension of same units and calendar as first variable but different times for first dimension and different masked data coord for 2nd dimension
    this_var.add_cids_for_fid(2, cids)
    thr.create_or_find_matching_variable(this_var) # this should not match to the 1st variable
    nvars+=1
    assert(len(thr.variables)==nvars)
    assert(thr.variables[nvars-1].vid==nvars-1)
    thr.variables[nvars-1].print()
    print('yet another two_d variable passed')

    # create var with no dimensions
    this_var=Variable_metadata(UNKNOWN_ID,'zero_d',0)
    this_var.attributes=[Attribute('long_name','zero_dimensional_variable')]
    cids=[] # no cid
    this_var.add_cids_for_fid(0, cids)
    thr.create_or_find_matching_variable(this_var)
    nvars+=1
    assert(len(thr.variables)==nvars)
    assert(thr.variables[nvars-1].vid==nvars-1)
    thr.variables[nvars-1].print()
    print('zero_d variable passed')

    for this_var in thr.variables:
        this_var.insert_into_database(thr.thread_name, thr.cur, thr.verbose)
    
    thr.con.commit()

    
def test_variables_from_database(thr):
    res=select_all_variables(thr.cur)
    assert(len(res)==len(thr.variables))
    vid=0
    print('--------------------------\nReading variables from database\n--------------------------')
    r=0
    for row in res:
        this_var=Variable_metadata(row, thr.cur)
        assert(this_var.vid==thr.variables[r].vid)
        assert(this_var.name==thr.variables[r].name)
        assert(this_var.ndims==thr.variables[r].ndims)
        assert(this_var.get_nfiles()==thr.variables[r].get_nfiles())
        assert(np.all((this_var.fids-thr.variables[r].fids)==0))
        for d in range(this_var.ndims):
            assert(np.all((this_var.get_cids_for_dim(d)-thr.variables[r].get_cids_for_dim(d))==0))
        r+=1
        this_var.print()
        if this_var.ndims==0:
            print('\tcids=',this_var.cids)
            assert(this_var.cids==[-1])

                                    
def main():

    dbname='unit_test.db'
    if os.path.exists(dbname):
        os.remove(dbname)

    Read_metadata_thread.verbose=False
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
        create_tables(Read_metadata_thread.cur, verbose=Read_metadata_thread.verbose)

    thr = Read_metadata_thread("","")
    
    test_coord_creation(thr)
    test_coords_from_database(thr)      
    test_variable_creation(thr)
    test_variables_from_database(thr)      
    thr.con.close()
    
if __name__ == '__main__':
    main()
