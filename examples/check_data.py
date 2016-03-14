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

from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.pyesgf_functions import *
from ARCCSSive.CMIP5.update_db_functions import *
from ARCCSSive.CMIP5.other_functions import *

# Should actually use parse_input() function from other_functions.py to build kwargs from input
#assign constraints
args=parse_input_check()

kwargs={"variable":["tas","tasmax"], "experiment":["rcp60","historical"],"model":["MIROC5"]}
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
    for o in outputs:
# loop available versions
       for v in o.versions:
# append to results list of version dictionaries containing useful info 
         db_results.append({'version':v.version,'files':v.files,'filenames':v.filenames2(), 'tracking_ids': v.tracking_ids()})
# search in ESGF database
    esgf.search_node(node_url,**constraints)
# loop returned DatasetResult objects
    for ds in esgf.get_ds():
# append to results list of version dictionaries containing useful info 
# NB search should return only one latest, not replica version if any
       esgf_results.append({'version': ds.get_attribute('version'), 'files':ds.files(), 'filenames':ds.filenames(),
                       'tracking_ids': ds.tracking_ids(), 'dataset_id':ds.dataset_id })
        

#  WHAT SHOULD WE DO IF THEER'S NOTHING ONLINE FOR A CERTAIN CONSTRAINTS COMB? JUST CONTINUE WITH LOOP OR ACTUALLY CHECK IF THERE'S ANYTHING ALREADY ON DATABASE BUT NOT UPDATING ANY INFO???
# FIRST AP[PROACH MAKE SENSE IF ALL I WANT IS TO COMPARE, BUT THEN IF POTENTIALLY A PARTICULAR VERSION HAS BEEN UNPUBLISHED OR IS CURRENTLY UNAVAILABLE I WOULD WANT TO KNOW
# PRINT A WARNING COULD BE SUFFICIENT? 
# compare local to remote info
    print("before esgf first result \n", esgf_results[0])
    if len(db_results)!= 0: print("before db first result \n", db_results[0])
    esgf_results, db_results=compare_instances(esgf_results, db_results)
    print("after esgf first result \n", esgf_results[0])
    if len(db_results)!= 0: print("after db first result \n", db_results[0])

# update database if needed based on recovered information, at the minimum for is_latest and checked_on
# potentially update file list and checksum if they're not there yet
# should we add an if admin clause here? 

# build table to summarise results
