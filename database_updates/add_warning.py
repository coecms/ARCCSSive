#!/usr/bin/env python
# This adds a warning to existing version.
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

from ARCCSSive.CMIP5 import connect
from ARCCSSive.CMIP5.Model import Instance, VersionWarning
from ARCCSSive.CMIP5.other_functions import combine_constraints, assign_mips
from ARCCSSive.CMIP5.update_db_functions import insert_unique
import argparse
from datetime import datetime 
import sys, os

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r'''Add warning message, user e-mail and current date to all
             the versions or group of matching the constraints.
             All arguments, except warning and user e-mail, can be repeated, for
            example to select two variables:
            -v tas tasmin
            At least one experiment, one model and mip should be passed all other
            arguments are optional.
            The script will add the warning to all the ensembles satifying the constraints
            [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...]
            AND [mip1 OR mip2 OR ...]
            Frequency adds all the correspondent mip_tables to the mip_table list.
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''',formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-a','--admin', action='store_true', default=False, help='running script as admin', required=False)
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=True)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=False)
    parser.add_argument('-t','--mip', type=str, nargs="*", help='CMIP5 MIP table', required=True)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    parser.add_argument('-w','--warning', type=str, nargs=1, help='warning message', required=True)
    parser.add_argument('-u','--email', type=str, nargs=1, help='user e-mail', required=True)
    return vars(parser.parse_args())


def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    admin = kwargs.pop("admin")
    warning = kwargs.pop("warning")[0]
    if len(warning) <= 10:
        print("Warning '",warning,"' is too short")
        sys.exit()
    email = kwargs.pop("email")[0]
    if "@" not in email:
        print(email, " is not a valid e-mail")
        sys.exit()
    # check if this is an authorised user
    if admin:
        if os.environ['USER'] not in ['pxp581','tae599']:
            print(os.environ['USER'] + " is not an authorised admin")
            sys.exit()
    frq = kwargs.pop("frequency")
    kwargs["mip"] = assign_mips(frq=frq,mip=kwargs["mip"])
    newkwargs=kwargs
    for k,v in list(kwargs.items()):
        if v is None or v==[]: newkwargs.pop(k)
    return newkwargs,admin,warning,email

# assign constraints from input
kwargs,admin,warning,email = assign_constraints()
# define directory where requests for downloads are stored
outdir="/g/data1/ua6/unofficial-ESG-replica/tmp/pxp581/requests/"

# open connection to local database and intiate SQLalchemy session 
cmip5 = connect()
db=cmip5.session

# if version/s defined take it out of constraints
vers_cnstr=None
if 'version' in kwargs.keys():
    vers_cnstr=kwargs.pop('version')
# get constraints combination
combs=combine_constraints(**kwargs)
# for each constraints combination
for constraints in combs:
    db_results=[]
    print(constraints,"versions=",vers_cnstr)
# search on local DB, return instance_ids
    if constraints['experiment']=='decadal':
        print("Warning: this will apply to all decadal experiments")
        exp0=constraints.pop('experiment')
        outputs=cmip5.outputs(**constraints).filter(Instance.experiment.like(exp0))
    else:
        outputs=cmip5.outputs(**constraints)
# if version/s defined in constraints filter results 
    if vers_cnstr:
        db_results=[v for o in outputs for v in o.versions if v.version in vers_cnstr]
        #db_results=[(v for v in o.versions if v.version in vers_cnstr) for o in outputs]
        print([v.version for v in db_results])
    else:
        db_results=[v for o in outputs for v in o.versions]
        print([v.version for v in db_results])
# print out summary of what will happen and check if user wants to go ahead
    if db_results==[]:
        print("No local version exists for constraints:\n",constraints,"versions=",vers_cnstr)
    else:
        print("Warning: \n '",warning,"'")
        print("will be added to the ",len(db_results),"versions returned from the database for constraints:\n",
               constraints,"versions=",vers_cnstr)
        if sys.version_info < ( 3, 0 ):
            request=raw_input("Do you want to proceed? Y/N \n")
        else:
            request=input("Do you want to proceed? Y/N \n")
        if request == "Y":
            if not admin:
                # if not admin write date/e-mail/message and list of version ids to file
                fout=open(outdir+"warning_"+os.environ['USER']+"_"+datetime.now().strftime("%Y%m%dT%H%M")+".txt", 'w')
                fout.write(",".join([datetime.now().strftime("%Y%m%d"),email,warning]))
                fout.write(","+",".join([str(v.id) for v in db_results]))
                fout.close()
            else:
                # if admin add warning directly to database, commit happens in function 
                for v in db_results:
                    insert_unique( db, VersionWarning, **{'version_id':v.id, 'warning':warning, 
                                 'added_by':email, 'added_on':datetime.now().strftime("%Y%m%d")} )
