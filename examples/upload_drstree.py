# a tentative script to upload all existing drstree "versions" into CMIP sqlite database
# each variable, mip, experiment, model, ensemble combination add a new instance in "instance"
# for each instance there should be at least one version in "version" table 
# for each version add at least one file in table "files" 

import argparse
from collections import defaultdict
from update_db_functions import *
#NB drstree root dir is also defined there
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

#kwargs={"institute":"BCC","model":"bcc-csm1-1-m", "experiment":"historical"}
kwargs=defaultdict(lambda: "*")
#kwargs=dict(model="IPSL-CM5A-MR", experiment="amip", mip="fx")
kwargs=dict(model="IPSL-CM5A-MR", experiment="amip", frequency="mon")

#loop through entire drstree or a subdir by using constraints **kwargs
instances=list_dir(drstree=drstree, **kwargs)
print instances
#for each instance individuated add instance row
for inst in instances:
# call file_details to retrieve experiment, variable, model etc. from filename
# call drs_details to retrieve model, experiment, freq. & realm (become mip), variable, ensemble from drstree path
# return dictionary
    # could i create an Instance, Version and file object instead and pass that on?
    kw_instance={}
    kw_version={}
    kw_files={}
    frequency, kw_instance = drs_details(inst)
    kw_instance['mip'] = get_mip(inst)
    #print kw_instance
# make sure details list isn't empty
    if kw_instance:
       versions = list_drs_versions(inst) 
       # add instance to db if not already existing
       inst_obj,new = insert_unique(db, Instance, **kw_instance)
       print(inst)
       print(inst_obj.id,new)
       #P use following two lines  if tmp/tree
       #kw_version['version'] = find_version(bits[:-1], version)
       #kw_version['path'] = '/'.join(bits[:-1])
       kw_version['instance_id'] = inst_obj.id
       for v in versions:
       # add version to db if not already existing
           kw_version['version'] = v
           files = list_drs_files(inst+"/"+v) 
           kw_version['path'] = tree_path("/".join([inst,v,files[0]])) 
           #print kw_version.items()
           v_obj,new = insert_unique(db, Version, **kw_version)
           print(v)
           print(v_obj.id,new)
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
                 kw_files['md5']=check_hash(v_obj.path+"/"+f,'md5')
                 insert_unique(db, VersionFile, **kw_files)
       

# need to have function to map bits of path to db instance fields!!
    #model,experiment,variable,mip,ensemble
    #kwargs[k]=
