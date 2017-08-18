#!/usr/bin/env python
# This search CMIP5 data available on raijin that matches constraints passed on by user and return paths for all available versions.
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

Last modified:
  2017/02/22 - added "decadal" option to be used as experiment input, it will return all experiment matching "decadal%"
             - fixed issue with Warning:"No local version exists for constraints .." appearing if one of the instances in database had no versions
             - corrected help text, it was referring to older version  
  2017/03/20 - added --all-versions / -a flag, it will return all versions not just latest 
  2017/03/24 - added "noVolc" option to be used as experiment input, it will return all experiment matching "noVolc%"
  2017/04/11 - added "warnings" -w option, it will return also the existing warnings in output file
"""

from __future__ import print_function

from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.other_functions import combine_constraints 
from ARCCSSive.CMIP5.Model import Instance
import argparse
import sys

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description='''Checks all the CMIP5 ensembles
             available on raijin, matching the constraints passed as arguments.
             Required arguments: experiment and variable .
             The others are optional and all can be repeated, except for the output filename:
            example to select two particular ensembles:
            -en r1i1p1 r2i1p1
            The script returns all the ensembles satifying the constraints
            var1 AND model1 AND exp1  AND mip1  AND other optional constraints.
            -e decadal / noVolc 
            will return all the experiments matching decadalYYYY / noVolcYYYY respectively
            -a will return all versions not only the latest
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=False)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=True)
    parser.add_argument('-t','--mip', type=str, nargs="*", help='CMIP5 MIP table', required=False)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    parser.add_argument('-a','--all-versions', help='returns all versions not only latest', action='store_true', 
                         required=False) 
    parser.add_argument('-w','--warnings', help='returns also warnings', action='store_true', 
                         required=False) 
    parser.add_argument('-o','--output', type=str, nargs=1, help='output file name', required=False)
    return vars(parser.parse_args())


def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    for k,v in kwargs.items():
        if v is None: kwargs.pop(k)
    return kwargs

def main():

    # Calling parse_input() function to build kwargs from external arguments passed by user 
    kwargs=assign_constraints()
    all_versions=kwargs.pop("all_versions")
    warnings=kwargs.pop("warnings")
    # open output file
    outfile=kwargs.pop("output",["search_result.txt"])
    fout=open(outfile[0],'w')

    # open connection to local database and intiate SQLalchemy session 
    cmip5 = CMIP5.connect()

    # get constraints combination
    combs=combine_constraints(**kwargs)
    # for each constraints combination
    for constraints in combs:
        db_results=[]
        print(constraints)
    # search on local DB, return instance_ids
    # allow "decadal" keyword to search for all decadalYYYY experiments
    # allow "noVolc" keyword to search for all noVolcYYYY experiments
        if constraints['experiment'] in ['decadal','noVolc']:
            exp0=constraints.pop('experiment')
            outputs=cmip5.outputs(**constraints).filter(Instance.experiment.like(exp0+"%"))
        else:
            outputs=cmip5.outputs(**constraints)
    # loop through returned Instance objects
        someresult=False
        for o in outputs:
            if all_versions:
                db_results=[v for v in o.versions]
            else:
                db_results=[v for v in o.versions if v==o.latest()[0]]
    # write result to output file
            if db_results!=[]:
                someresult=True
                for v in db_results:
                    fout.write(",".join([o.experiment,o.variable,o.mip,o.model,o.ensemble,v.version,v.path]) + "\n")
                    if warnings and v.warnings != []:
                        fout.write("Warnings:\n")
                        for w in v.warnings:
                            fout.write(w.warning + "\n added by " + w.added_by + " on the " + w.added_on + "\n")
        if not someresult:
             print("No local version exists for constraints:\n",constraints)
    fout.close()
           
if __name__ == '__main__':
    # check python version and then call main()
    if sys.version_info < ( 2, 7):
        # python too old, kill the script
        sys.exit("This script requires Python 2.7 or newer!")

    main()
