# This search CMIP5 data available on raijin that matches constraints passed on by user and return paths for all available versions.
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
from ARCCSSive.CMIP5.update_db_functions import *
from ARCCSSive.CMIP5.other_functions import *
from collections import defaultdict
import sys

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r'''Checks all the CMIP5 ensembles
             available on raijin, matching the constraints passed as arguments.||
             Accept one value for the required arguments: experiment, model, variable, mip.
             The others are optional and can be repeated:
            example to select two particular ensembles:||
            -e r1i1p1 r2i1p1||
            The script returns all the ensembles satifying the constraints
            var1 AND model1 AND exp1  AND mip1  AND other optional constraints||
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''')
    parser.add_argument('-e','--experiment', type=str, nargs=1, help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs=1, help='CMIP5 model', required=True)
    parser.add_argument('-v','--variable', type=str, nargs=1, help='CMIP5 variable', required=True)
    parser.add_argument('-t','--mip', type=str, nargs=1, help='CMIP5 MIP table', required=True)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    parser.add_argument('-c','--checksum', type=str, nargs=1, help='checksum_type: md5 or sha256', required=False)
    parser.add_argument('-o','--output', type=str, nargs=1, help='output file name', required=False)
    return vars(parser.parse_args())


def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    for k,v in kwargs.items():
        if v is None: kwargs.pop(k)
    return kwargs

# Calling parse_input() function to build kwargs from external arguments paased by user 
kwargs=assign_constraints()
# open output file
outfile=kwargs.pop("output",None)
if outfile is None: outfile="search_result.txt"
fout=open(outfile[0],'w')
# if checksum_type has been passed add checksum to output
checksum=False
cks = kwargs.pop("checksum",["None"])
if cks[0] in  ["md5","sha256"]:
   checksum=True
   cks_type=cks[0]

# a list fo the standard unique constraints defining one instance in the database
# open connection to local database and intiate SQLalchemy session 
cmip5 = CMIP5.connect()

# get constraints combination
combs=combine_constraints(**kwargs)
# for each constraints combination
for constraints in combs:
    db_results=[]
    print(constraints)
# search on local DB, return instance_ids
    outputs=cmip5.outputs(**constraints)
# loop through returned Instance objects
    db_results=[v for o in outputs for v in o.versions if v.is_latest]
    if db_results==[]:
       db_results=[v for o in outputs for v in o.versions if v.version==o.latest()[1]]
# write result to output file
    if db_results==[]:
       print("No local version exists for constraints:\n",constraints)
    else:
       for v in db_results:
          fout.write(v.version + ", checksum: " + cks[0] + "\n")
          vpath=v.path + "/"
          if checksum:
             fout.writelines(vpath + f.filename + ", " + str(f.__getattribute__(cks_type)) + "\n" for f in v.files)
          else:
             fout.writelines(vpath + f.filename + "\n" for f in v.files)
fout.close()
       

