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


def check_netcdf(fpath):
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
    try:
        f.close()
    except:
        pass
    return version


# open local database using ARCSSive interface
conn = DB.connect()
db = conn.session
flist = "$HOME/fileslist.csv"

#loop through entire drstree or a subdir by using constraints **kwargs
# variable,mip_table,model,experiment,ensemble,realm,version,path

instances=list_tmpdir(flist)
#for each instance individuated add instance row
for kw_instance in instances:
# create dictionary of fields for new instance
    kw_version={}
    kw_files={}
    kw_version['version'] = kw_instance.pop('version')
    kw_version['path'] = kw_instance.pop('path')
    if kw_version['version']=='NA':
       fpath=os.listdir(kw_version['path'])[0]
       fversion = check_netcdf(fpath)
       if fversion: 
           kw_version['version']= fversion
    print(kw_version['path'])
# add instance to database if does not exist yet
    inst_obj,new = insert_unique(db, Instance, **kw_instance)
# create dictionary of fields for new version
    kw_version['instance_id'] = inst_obj.id
# retrieve all files for version 
    files = list_drs_files(kw_version['path']) 
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
    else:
# if some files exist use insert_unique instead 
        kw_files['version_id']=v_obj.id
        for f in files:
            kw_files['filename']=f
            kw_files['sha256']=check_hash(v_obj.path+"/"+f,'sha256')
            kw_files['tracking_id']=get_trackid(v_obj.path+"/"+f)
            insert_unique(db, VersionFile, **kw_files)
       
