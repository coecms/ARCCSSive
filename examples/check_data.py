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

def parse_input_check():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r'''Checks all the CMIP5 ensembles
             (latest official version) on ESGF nodes, matching the constraints
             passed as arguments and compare them to ones available on raijins.||
             All arguments, except the output file name,  can be repeated, for
            example to select two variables:||
            -v tas tasmin||
            At least one experiment and one variable should be passed all other
            arguments are optional.||
            The script returns all the ensembles satifying the constraints
            [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...]
            AND [mip1 OR mip2 OR ...]||
            Frequency adds all the correspondent mip_tables to the mip_table list.||
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''')
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=False)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=True)
    parser.add_argument('-t','--mip_table', type=str, nargs="*", help='CMIP5 MIP table', required=False)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    parser.add_argument('-o','--output', type=str, nargs=1, help='database output file name', required=False)
    return vars(parser.parse_args())


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
             files.append(f)
             checksums.append(f.checksum)
             tracking_ids.append(f.tracking_id)
       esgf_results.append({'version': "v" + ds.get_attribute('version'), 
             'files':files, 'tracking_ids': tracking_ids, 
             'checksum_type': ds.chksum_type(), 'checksums': checksums,
             'dataset_id':ds.dataset_id })
        
# compare local to remote info
    if esgf_results==[]:
       if db_results!=[]:
           print("Found local version but none is currently available on ESGF nodes for constraints:\n",constraints)
       else: 
           print("Nothing currently available on ESGF nodes and no local version existsi for constraints:\n",constraints)
    else:
       esgf_results, db_results=compare_instances(cmip5.session, esgf_results, db_results)

# build table to summarise results
