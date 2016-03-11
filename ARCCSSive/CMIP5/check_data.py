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

import os

#from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile, VersionWarning
from ARCCSSive import CMIP5
from pyesgf_functions import *
from update_db_functions import *
from other_functions import *

#assign constraints
kwargs={"variable":["tas","tasmax"], "experiment":["rcp60","historical"],"model":["MIROC5"]}
# open connection to ESGF
node_url="http://pcmdi.llnl.gov/esg-search"
openid="https://pcmdi.llnl.gov/esgf-idp/openid/paolap2"
password="FM27g201@"
#Psession=logon(openid,password)
#Pif not session.is_logged_on():
#P   print("User ", openid.split("/")[-1], "could not log onto node ", openid.split("/")[2])
#Pelse:
#P   print("User successfully logged on ESGF")
#open connection to local database
cmip5 = CMIP5.connect()

# get constraints combination
combs=combine_constraints(**kwargs)
# use function already written to add in other_functions.py
esgf=ESGFSearch()
##or should I use here a dictionary again to have 
# search constraints on local DB, these should return instance_ids
# search versions available for instance_ids returned
for constraints in combs:
    db_results=[]
    esgf_results=[]
    print(constraints)
    outputs=cmip5.outputs(**constraints)
# this is just a test
    for o in outputs:
       for v in o.versions:
         print(v.id,v.is_latest)
# either I append the entire object or i append only the useful info
         db_results.append({'version':v.version,'files':v.files,'filenames':v.filenames2(), 'tracking_ids': v.tracking_ids()})
# search combs in ESGF database
    esgf.search_node(node_url,**constraints)
    for ds in esgf.get_ds():
       #esgf_results.append(ds)
       esgf_results.append({'version': ds.get_attribute('version'), 'files':ds.files(), 'filenames':ds.filenames(),
                       'tracking_ids': ds.tracking_ids(), 'dataset_id':ds.dataset_id })
        
    #print(esgf_results)
#    print(db_results)

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
