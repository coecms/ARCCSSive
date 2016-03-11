# a tentative script to upload all existing drstree "versions" into CMIP sqlite database
# each variable, mip, experiment, model, ensemble combination add a new instance in "instance"
# for each instance there should be at least one version in "version" table 
# for each version add at least one file in table "files" 

import argparse
from collections import defaultdict
from update_db_functions import *
#NB tmptree root dir is also defined there
from ARCCSSive import CMIP5


def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description='''Lists all the CMIP5 ensembles available on raijin and
             responding to the constraints passed as arguments.
            All arguments, except the output file name,  can be repeated, for example to select two variables:
            -v tas tasmin
            All arguments are optional, failing to input any argument will return the entire dataset.
            The script returns all the ensembles satifying the constraints
            [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...]
            AND [mip1 OR mip2 OR ...]
            Frequency adds all the correspondent mip_tables to the mip_table list
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''')
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=False)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=False)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=False)
    parser.add_argument('-t','--mip_table', type=str, nargs="*", help='CMIP5 MIP table', required=False)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    parser.add_argument('-o','--output', type=str, nargs=1, help='database output file name', required=False)
    return vars(parser.parse_args())

# open local database using ARCSSive interface
conn = CMIP5.DB.connect()
db = conn.session
flist = "/home/581/pxp581/allcmip5data.csv"

#loop through entire drstree or a subdir by using constraints **kwargs
# variable,mip_table,model,experiment,ensemble,realm,version,path

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
    if v_obj.filenames==[]: 
       rows=[]
       for f in files:
           checksum=check_hash(v_obj.path+"/"+f,'md5')
           rows.append(dict(filename=f, md5=checksum, version_id=v_obj.id))
       add_bulk_item(db, VersionFile, rows)
    else:
           kw_files['version_id']=v_obj.id
           for f in files:
               kw_files['filename']=f
               #kw_files['md5']=check_hash(v_obj.path+"/"+f,'md5')
               insert_unique(db, VersionFile, **kw_files)
       
