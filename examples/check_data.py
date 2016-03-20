# This check data available on ESGF and on raijin that matches constraints passed on by user and return a summary.
#!/usr/bin/env python
"""
Copyright 2016 ARC Centre of Excellence for Climate Systems Science

author: Paola Petrelli <paola.petrelli@utas.edu.au>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import print_function
#from __future__ import unicode_literals

import timing
from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.pyesgf_functions import *
from ARCCSSive.CMIP5.update_db_functions import *
from ARCCSSive.CMIP5.other_functions import *

def assign_constraint():
    ''' Assign default values and input to constraints '''
    global var0, exp0, mod0, table, outfile, db, dbfile
    var0 = []
    exp0 = []
    mod0 = []
    outfile = 'variables'
# assign constraints from arguments list
    args = parse_input()
    var0=args["variable"]
    if args["model"]: mod0=args["model"]
    exp0=args["experiment"]
    table=args["table"]
    db=args["database"]
    if db: table=True
    outfile=args["output"]
    return


# Should actually use parse_input() function from other_functions.py to build kwargs from input
#assign constraints
args=parse_input_check()
timing.log("finished argparse")

kwargs={"ensemble":["r4i1p1"],"mip":["Amon"], "variable":["tasmax"], "experiment":["historical"],"model":["MIROC5"]}
# open connection to local database and intiate SQLalchemy session 
cmip5 = CMIP5.connect()

# get constraints combination
combs=combine_constraints(**kwargs)
# initiate ESGFsearch object 
esgf=ESGFSearch()
# for each constraints combination
for constraints in combs:
    db_results=[]
    esgf_results=[]
    print(constraints)
# search on local DB, return instance_ids
    outputs=cmip5.outputs(**constraints)
# loop through returned Instance objects
    db_results=[v for o in outputs for v in o.versions]
    #Pfor o in outputs:
    #P   print("instance_id: ",o.id)
# loop available versions
    #P   for v in o.versions:
    #P     print("version_id: ",v.id)
    #P     print("files: ",v.files,v.tracking_ids())
# append to results list of version dictionaries containing useful info 
         #Pdb_results.append({'version':v.version,'vid':v.id,'files':v.files,'path':v.path, 
         #P                   'tracking_ids': v.tracking_ids(),'dataset_id':v.dataset_id,
         #P                    'is_latest': v.is_latest})
         #Pprint(v.id,[f.sha256 for f in v.files])
    timing.log("finished DB search")
# search in ESGF database
    constraints['distrib']=False 
    kwargs=constraints
    constraints['cmor_table']=constraints.pop('mip')
    esgf.search_node(**constraints)
# loop returned DatasetResult objects
    for ds in esgf.get_ds():
# append to results list of version dictionaries containing useful info 
# NB search should return only one latest, not replica version if any
       files, checksums, tracking_ids = [],[],[]
       for f in ds.files(): 
          if f.get_attribute('variable')[0]== constraints['variable']:
             print('found same var as tasmax',f.checksum,f.tracking_id,f)
             files.append(f)
             checksums.append(f.checksum)
             tracking_ids.append(f.tracking_id)
          #print(checksums,tracking_ids)
       esgf_results.append({'version': "v" + ds.get_attribute('version'), 
             'files':files, 'tracking_ids': tracking_ids, 
             'checksum_type': ds.chksum_type(), 'checksums': checksums,
             'dataset_id':ds.dataset_id })
    timing.log("finished ESGF search")
        

#  WHAT SHOULD WE DO IF THEER'S NOTHING ONLINE FOR A CERTAIN CONSTRAINTS COMB? JUST CONTINUE WITH LOOP OR ACTUALLY CHECK IF THERE'S ANYTHING ALREADY ON DATABASE BUT NOT UPDATING ANY INFO???
# FIRST AP[PROACH MAKE SENSE IF ALL I WANT IS TO COMPARE, BUT THEN IF POTENTIALLY A PARTICULAR VERSION HAS BEEN UNPUBLISHED OR IS CURRENTLY UNAVAILABLE I WOULD WANT TO KNOW
# PRINT A WARNING COULD BE SUFFICIENT? 
# compare local to remote info
    esgf_results, db_results=compare_instances(cmip5.session, esgf_results, db_results)
    print("after esgf results \n", esgf_results)
    if len(db_results)!= 0: print("after db results \n", db_results)

# update database if needed based on recovered information, at the minimum for is_latest and checked_on
# potentially update file list and checksum if they're not there yet
# should we add an if admin clause here? 

# build table to summarise results