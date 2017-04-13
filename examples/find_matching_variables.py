#!/usr/bin/env python
# This script returns all models/ensembles combination available on raijin that have "all" the selected variables for 
# any combination of the constraints passed by the user. 
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
from ARCCSSive.CMIP5.Model import Instance
from ARCCSSive.CMIP5.other_functions import assign_mips, combine_constraints, unique 
from collections import defaultdict
import argparse
import sys
from sqlalchemy import and_

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r'''Returns models/ensembles 
             that have all the listed variables for any combination of the selected constraints
             experiment/mip/frequency/version. Please note that model and ensemble can
             also be passed to further constraint selection. Returns also version if available. 
             All arguments, except the output file name,  can be repeated, for
            example to select two variables:
            -v tas tasmin
            At least one experiment, one mip and one variable should be passed all other
            arguments are optional.
            The script returns all the ensembles satifying the constraints
            [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...]
            AND [mip1 OR mip2 OR ...]
            Frequency adds all the correspondent mip_tables to the mip_table list.
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''',formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=False)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=True)
    parser.add_argument('-t','--mip', type=str, nargs="*", help='CMIP5 MIP table', required=True)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    return vars(parser.parse_args())


def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    frq = kwargs.pop("frequency")
    kwargs["mip"] = assign_mips(frq=frq,mip=kwargs["mip"])
    newkwargs=kwargs
    for k,v in list(kwargs.items()):
        if v is None or v==[]: newkwargs.pop(k)
    return newkwargs


# assign constraints from input
kwargs=assign_constraints()

# initialise dictionary to contain a list of results
results=defaultdict(list)
# open connection to local database and intiate SQLalchemy session 
cmip5 = connect()
# remove variables from constraints dictionary and save it as a separate list
variables=kwargs.pop("variable")
# get constraints combination
combs=combine_constraints(**kwargs)
# for each constraints combination
for constraints in combs:
    exp0=constraints['experiment']
    mip0=constraints['mip']
# search on localdatabase for matching Instances, if key "decadal" used for experiments, do this in two steps  
# first search all other constraints, then filter results where experiment=="decadal*"
# in both cases filter for variables which belong to input list
    if constraints['experiment']=='decadal':
        outputs=cmip5.outputs(**constraints).filter(and_(Instance.experiment.like(exp0), Instance.variable.in_(variables)))
    else:
        outputs=cmip5.outputs(**constraints).filter(Instance.variable.in_(variables))
#extract list of models from Instances returned by search 
    models=unique(outputs,'model')
# build string to use as result dict key
    str_constr="_".join([constraints["experiment"],constraints["mip"]])
# filter results by model, then ensembles and check if all variables are present
# save details in versions dictionary as a string joining "model ensemble variable" as key and a list of versions as value
    for mod0 in models:
        mod_outs=outputs.filter(Instance.model == mod0)
        ensembles=unique(mod_outs,'ensemble')
        for ens0 in ensembles:
            varset=set()
            res=mod_outs.filter(Instance.ensemble==ens0)
            versions=defaultdict(set)
            for o in res:
                versions[" ".join([mod0,ens0,str(o.variable)])].update(str(v.version) for v in o.versions)
                varset.add(str(o.variable))
                # find version for each variable
            if set(variables).issubset(varset):
      #  if all variables  are available for an esemble/model pair than save in results dict else pass to next ensemble/model
                results[str_constr].append(versions)
    
# this is a bit ugly but works
# for each model/ensemble pair in results if there saved results print them
for k,v in results.items():
    if v!=[]:
        print("Found model/s for constraints: ",k)
        for d in v:
            for k2,v2 in d.items():
                print(k2,": ",",".join(v2))
    else: 
        print("Found no models with all variables available for constraints:\n",k)

