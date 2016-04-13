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
from ARCCSSive.CMIP5.pyesgf_functions import *
from ARCCSSive.CMIP5.update_db_functions import *
from ARCCSSive.CMIP5.other_functions import *




#assign constraints
kwargs={"variable":["tas","tasmin"], "experiment":["rcp45","historical"],"model":["MIROC5"]}
# open connection to ESGF
node_url="http://pcmdi.llnl.gov/esg-search"
openid="https://pcmdi.llnl.gov/esgf-idp/openid/"
password=""
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
db_results=[]
esgf_ds=[]
esgf=ESGFSearch()
#print(type(esgf))
#print(dir(esgf))
##or should I use here a dictionary again to have 
for constraints in combs:
    print(constraints)
    db_results.append(cmip5.outputs(**constraints))
# this is just a test
for o in db_results[0]:
    print(o.__table_args__[0].columns.keys())
    print(type(Instance.__table_args__[0].columns.keys()))
    for v in o.versions:
       ukeys=[x for x in v.variable.__dict__.keys() if True]
       print(v.id,v.variable.__dict__.keys())
     #  print(dir(v))
# search combs in ESGF database
    esgf.search_node(**constraints)
#    print(esgf.ds_count())
#    print(esgf.ds_ids())
#    print(esgf.ds_variables())
#    print(esgf.ds_versions())
    print(dir(esgf),type(esgf))
    print(esgf.facet_options())
    for k,v in esgf.facet_options().items():
         print(k,v.keys())
 #   print(esgf.which_facets('frequency'))
 #   print(esgf.facets())
# not sure this is working either!!!
    esgf.ds_filter(ensemble='r1i1p1')
    print(esgf.ds_ids())
    esgf_results.append(esgf.ctx.search())
#    for ds in esgf.get_ds():
# this is just a test
# now I'm getting the DatasetResult objects
for ds in esgf_ds:
#        print(ds.dataset_id)
        print(dir(ds))
        #print(ds.urls)
#        esgf_ds.append(ds)
#        for f in ds.files():
#           print(f.download_url)
#print(esgf_ds)



# close connection to ESGF
#Pif logoff(session):
#P   print("User successfully logged off ESGF")
#Pelse:
#P   print("User could not log off ESGF")
# collate info from ESGF database in dict??
# should build function for this

# search constraints on local DB, these should return instance_ids

# search versions available for instance_ids returned

# collate versions info
# should build function for this potentially independent of which DB infor comes out

# compare local to remote info

# update database if needed based on recovered information, at the minimum for is_latest and checked_on
# potentially update file list and checksum if they're not there yet
# should we add an if admin clause here? 

# build table to summarise results
