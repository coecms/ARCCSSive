# a tentative script to upload all existing drstree "versions" into CMIP sqlite database
# each variable, mip, experiment, model, ensemble combination add a new instance in "instance"
# for each instance there should be at least one version in "version" table 
# for each version add at least one file in table "files" 

import argparse
from multiprocessing import Pool
from collections import defaultdict
import itertools

from update_db_functions import *
#NB tmptree root dir is also defined there
from ARCCSSive import CMIP5



# open local database using ARCSSive interface
conn = CMIP5.DB.connect()
db = conn.session
#flist = "/home/581/pxp581/allcmip5data.csv"
flist = "/home/581/pxp581/sub.csv"

#loop through entire drstree or a subdir by using constraints **kwargs
# variable,mip_table,model,experiment,ensemble,realm,version,path

def process_file(arg_list):
    ''' take file path on id and checksum type list as input and calculates checksum returning a dict for each file ''' 
    #f,vid,check_type = arg_list
    f,vid = arg_list
    checksum=check_hash(v_obj.path+"/"+f,'md5')
    return dict(filename=f, md5=checksum, version_id=vid)

instances=list_tmpdir(flist)
#print instances
#for each instance individuated add instance row
for kw_instance in instances:
# return dictionary
    # could i create an Instance, Version and file object instead and pass that on?
    kw_version={}
    kw_files={}
    kw_version['version'] = kw_instance.pop('version')
    kw_version['path'] = kw_instance.pop('path')
    print(kw_version['path'])
    #frequency = mip_dict(kw_instance['mip'])
# make sure details list isn't empty
    inst_obj,new = insert_unique(db, Instance, **kw_instance)
    kw_version['instance_id'] = inst_obj.id
    files = list_drs_files(kw_version['path']) 
    #print kw_version.items()
    v_obj,new = insert_unique(db, Version, **kw_version)
    if v_obj.filenames2()==[]: print("no files for version ",v_obj.id)
    if v_obj.filenames2()==[]: 
       #rows=[]
       combs=itertools.product(*[files,[v_obj.id]])
       arg_list=[x for x in combs]
       #print(arg_list)
       async_results = Pool(16).map_async(process_file, arg_list)
       rows=[x for x in async_results.get()]
       print(rows)
       add_bulk_items(db, VersionFile, rows)
    else:
           print("I'm here")
           kw_files['version_id']=v_obj.id
           for f in files:
               kw_files['filename']=f
               kw_files['md5']=check_hash(v_obj.path+"/"+f,'md5')
               insert_unique(db, VersionFile, **kw_files)
       
