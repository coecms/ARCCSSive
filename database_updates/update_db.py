# a tentative script to upload all existing drstree "versions" into CMIP sqlite database
# each variable, mip, experiment, model, ensemble combination add a new instance in "instance"
# for each instance there should be at least one version in "version" table 
# for each version add at least one file in table "files" 

from __future__ import print_function

import argparse
from ARCCSSive.CMIP5.update_db_functions import insert_unique, add_bulk_items 
from ARCCSSive.CMIP5.other_functions import list_logfile, list_drs_files, check_hash, get_trackid 
#NB tmptree root dir is also defined there
from ARCCSSive.CMIP5 import DB 
from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile 
import cdms2
import os,sys
import glob


def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r'''Update database using the
             logs produced by compare_ESGF for new ensembles
             to run:
             python update_db.py -i <input-file1> <input-file2>
             At least one input file must be passed as argument.''',formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i','--input_file', type=str, nargs="*", help='input file with dataset information', required=True)
    return vars(parser.parse_args())


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


# assign input arguments
kwargs = parse_input()
ifiles = kwargs['input_file']
for f in ifiles:
    if '*' in f:
        ifiles.remove(f)
        ifiles.extend(glob.glob(f)) 
# open local database using ARCSSive interface
cmip5 = DB.connect()
db = cmip5.session
# initiliase instances as empty list
instances=[]
# for each file read info into a list of dictionary containing dataset info
# each dict has keys:
# variable, mip, model, experiment, ensemble, realm, version, path, chk_type, files
# where files is a dict with keys: filename, tracking_id, checksum
for inf in ifiles:
    flist = inf 
    instances.extend(list_logfile(flist))
#for each instance individuated add instance row
for kw_instance in instances:
# create dictionary of fields for new instance
    var=kw_instance['variable']
    kw_version={}
    kw_files={}
    kw_version['version'] = kw_instance.pop('version')
    kw_version['dataset_id'] = kw_instance.pop('dataset_id')
    vers_path = kw_instance.pop('path')
    kw_version['path'] = vers_path
    print(vers_path)
    ctype = kw_instance.pop('cks_type').replace("\n","")
    if ctype=="None":
        ctype='sha256'
    kw_files = kw_instance.pop('files')
    if kw_instance['realm']=='NA':
        fpaths=[p for p in os.listdir(vers_path) if p.split("_")[0]==var]
        realm=check_realm(vers_path+"/"+fpaths[0])
    if len(kw_version['version']) < 9:
        fpaths=[p for p in os.listdir(kw_version['path']) if p.split("_")[0]==var]
        fversion=check_version(vers_path+"/"+fpaths[0])
        if fversion: 
            kw_version['version']= fversion
        else:
            kw_version['version']= "NA" 
# add instance to database if does not exist yet
    inst_obj,new = insert_unique(db, Instance, **kw_instance)
    print("instance:",inst_obj.id,new)
# create dictionary of fields for new version
    kw_version['instance_id'] = inst_obj.id
# add version to database if does not exist yet
    v_obj,new = insert_unique(db, Version, **kw_version)
    print("version:",v_obj.id,new)
# check if files objects exist already if not add from files dictionary 
# add both tracking-ids and checksums, if checksums are "None" calculate sha256
    for i,f in enumerate(kw_files):
        if f['checksum']=="None":
            kw_files[i][ctype]=check_hash(v_obj.path+"/"+f['filename'],ctype)
            f.pop('checksum')
        else:
            kw_files[i][ctype]=f.pop('checksum')
        if f['tracking_id']=="":
            kw_files[i]['tracking_id']=get_trackid(v_obj.path+"/"+f['filename'])
        kw_files[i]['version_id']=v_obj.id
# add files to database with bulk insert
    if v_obj.filenames()==[]: 
        add_bulk_items(db, VersionFile, kw_files)
# if some files exist already use insert_unique instead 
    else:
        for i,f in enumerate(kw_files):
            insert_unique(db, VersionFile, **f)
       
