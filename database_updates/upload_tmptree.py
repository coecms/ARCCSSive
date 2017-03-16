# a tentative script to upload all existing drstree "versions" into CMIP sqlite database
# each variable, mip, experiment, model, ensemble combination add a new instance in "instance"
# for each instance there should be at least one version in "version" table 
# for each version add at least one file in table "files" 

from __future__ import print_function

#import argparse
from ARCCSSive.CMIP5.update_db_functions import insert_unique, add_bulk_items 
from ARCCSSive.CMIP5.other_functions import list_tmpdir, list_drs_files, check_hash, get_trackid 
#NB tmptree root dir is also defined there
from ARCCSSive.CMIP5 import DB 
from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile 
import cdms2
import os


def check_version(fpath):
    ''' Check for version and/or creation date in netcdf file '''
    # open netcdf file
    try:
        f = cdms2.open(fpath,'r')
    except:
        print("INVALID NETCDF,%s" % fpath)
        return None 
    # read attributes
    try:
        version=f.version_number
    except:
        version=None
    f.close()
    return version


def check_realm(fpath):
    ''' Check for realm in netcdf file '''
    # open netcdf file
    try:
        f = cdms2.open(fpath,'r')
    except:
        print("INVALID NETCDF,%s" % fpath)
        return None 
    # read attributes
    try:
        realm=f.modeling_realm
    except:
        realm=None
    f.close()
    return realm 



# open local database using ARCSSive interface
cmip5 = DB.connect()
db = cmip5.session
#flist = "fileslist.csv"
flist = "/home/581/pxp581/Sep15diff.csv"

#loop through entire drstree or a subdir by using constraints **kwargs
# variable,mip_table,model,experiment,ensemble,realm,version,path

instances=list_tmpdir(flist)
#for each instance individuated add instance row
for kw_instance in instances:
# create dictionary of fields for new instance
    var=kw_instance['variable']
    kw_version={}
    kw_files={}
    kw_version['version'] = kw_instance.pop('version')
    vers_path = kw_instance.pop('path')
    kw_version['path'] = vers_path
    print(vers_path)
    if kw_instance['realm']=='NA':
        fpaths=[p for p in os.listdir(vers_path) if p.split("_")[0]==var]
        realm=check_realm(vers_path+"/"+fpaths[0])
    if kw_version['version']=='NA':
        fpaths=[p for p in os.listdir(kw_version['path']) if p.split("_")[0]==var]
        fversion=check_version(vers_path+"/"+fpaths[0])
        if fversion: 
            kw_version['version']= fversion
    print(kw_version['path'])
# add instance to database if does not exist yet
    inst_obj,new = insert_unique(db, Instance, **kw_instance)
# create dictionary of fields for new version
    kw_version['instance_id'] = inst_obj.id
# retrieve all files for version 
    allfiles = list_drs_files(vers_path) 
# loop through allfiles and make sure only the instance variable is present (ie GISS is breaking CMOR rules)
    files=[]
    for f in allfiles:
        if var == f.split("_")[0]:
            files.append(f)
# add version to database if does not exist yet
    v_obj,new = insert_unique(db, Version, **kw_version)
# check if files objects exist already if not save details in list of dictionaries (rows)
# add both tracking-ids and sha256 checksums
    if v_obj.filenames()==[]: 
        rows=[]
        for f in files:
            checksum=check_hash(v_obj.path+"/"+f,'sha256')
            trackid=get_trackid(v_obj.path+"/"+f)
            rows.append(dict(filename=f, sha256=checksum, tracking_id=trackid, version_id=v_obj.id))
# add files to database with bulk insert
        add_bulk_items(db, VersionFile, rows)
# if some files exist already use insert_unique instead 
    else:
        kw_files['version_id']=v_obj.id
        for f in files:
            kw_files['filename']=f
            kw_files['sha256']=check_hash(v_obj.path+"/"+f,'sha256')
            kw_files['tracking_id']=get_trackid(v_obj.path+"/"+f)
            insert_unique(db, VersionFile, **kw_files)
       
