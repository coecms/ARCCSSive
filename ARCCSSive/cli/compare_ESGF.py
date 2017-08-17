#!/usr/bin/env python
# This check data available on ESGF and on raijin that matches constraints passed on by user and return a summary.
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
from ARCCSSive.CMIP5.pyesgf_functions import ESGFSearch 
from ARCCSSive.CMIP5.other_functions import assign_mips, combine_constraints 
from ARCCSSive.CMIP5.compare_helpers import *
import sys
from multiprocessing import Pool

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r'''Checks all the CMIP5 ensembles
             (latest official version) on ESGF nodes, matching the constraints
             passed as arguments and compare them to ones available on raijins.
             All arguments, except the output file name,  can be repeated, for
            example to select two variables:
            -v tas tasmin
            At least one experiment and one variable should be passed all other
            arguments are optional.
            The script returns all the ensembles satifying the constraints
            [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...]
            AND [mip1 OR mip2 OR ...]
            Frequency adds all the correspondent mip_tables to the mip_table list.
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.
            The additional arguments replica, node and project modify the main ESGF search parameters. Defaults are
            no replicas, PCMDI node and CMIP5 project. If you chnage project you need to export a different local database,
            currently only geomip is available:
               export CMIP5_DB=sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/geomip_latest.db 
            NB this script uses Pool module to parallelise downloading information from the ESGF.
               You can choose to use more than one cpu by changing parameter at line 247 from 1 to ncpus available.
               Please do not do this on raijin unless you're submitting job to queue.''',formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-a','--admin', action='store_true', default=False, help='running script as admin', required=False)
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=False)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=True)
    parser.add_argument('-t','--mip', type=str, nargs="*", help='CMIP5 MIP table', required=False)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    parser.add_argument('-r','--replica', help='search also replica', action='store_true', required=False)
    parser.add_argument('-n','--node', type=str, help='ESGF node to use for search', required=False)
    parser.add_argument('-p','--project', type=str, help='ESGF project to search', required=False)
    return vars(parser.parse_args())

def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    admin = kwargs.pop("admin")
    searchargs={}
    searchargs['replica'] = kwargs.pop("replica")
    searchargs['project'] = kwargs.pop("project")
    searchargs['node'] = kwargs.pop("node")
    # this can be only changed manuallyi, default True means that the search is distributed across all ESGF nodes
    #searchargs['distrib'] = True
    variables = kwargs.pop("variable")
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
    for k,v in list(searchargs.items()):
        if v is None or v==[]: searchargs.pop(k)
    return newkwargs, variables, admin, searchargs

def main():

    # assign constraints from input
    kwargs, variables, admin, searchargs = assign_constraints()
    # define directory where requests for downloads are stored
    outdir="/g/data1/ua6/unofficial-ESG-replica/tmp/pxp581/requests/"

    # a list fo the standard unique constraints defining one instance in the database
    # initialize dictionary of exp/matrices
    matrix=defaultdict(lambda: defaultdict(list))
    # open connection to local database and intiate SQLalchemy session 
    cmip5 = connect()

    # get constraints combination
    combs=combine_constraints(**kwargs)
    # initiate ESGFsearch object 
    esgf=ESGFSearch()
    # for each constraints combination
    for constraints in combs:
        db_results=[]
        esgf_results=[]
        print(constraints)
        print(variables)
        orig_args=constraints.copy()
    # search on local DB, return instance_ids
        if constraints['experiment'] in ['decadal','noVolc']:
            exp0=constraints.pop('experiment')
            outputs=cmip5.outputs(**constraints).filter(Instance.experiment.like(exp0+"%")).filter(Instance.variable.in_(variables))
        else:
            outputs=cmip5.outputs(**constraints).filter(Instance.variable.in_(variables))
    # loop through returned Instance objects
        db_results=[v for o in outputs for v in o.versions]
    # search in ESGF database
    # you can use the key 'distrib'=False to search only one node 
    # for more info look at pyesgf module documentation
        esgfargs=searchargs.copy()
        esgfargs.update(constraints)
        if 'mip' in constraints.keys():
            esgfargs['cmor_table']=esgfargs.pop('mip')
        if 'exp0' in locals():
            esgfargs['query']=exp0+"%"
        esgf.search_node(**esgfargs)
        print("Found ",esgf.ds_count(),"simulations for constraints")
    # loop returned DatasetResult objects
    # using multiprocessing Pool to parallelise process_file
    # using 8 here as it is the number ov VCPU on VDI
        if esgf.ds_count()>=1:
            results=[(ds,variables) for ds in esgf.get_ds()]
            async_results = Pool(1).map_async(retrieve_ds, results)
            for ds_info in async_results.get():
                esgf_results.extend(ds_info)
    # append to results list of version dictionaries containing useful info 
    # NB search should return only one latest, not replica version if any
            
    # compare local to remote info
        print("Finished to retrieve remote data")
        if esgf_results==[]:
            if db_results!=[]:
                print("Found local version but none is currently available on ESGF nodes for constraints:\n",constraints,"and variables:",variables)
            else: 
                print("Nothing currently available on ESGF nodes and no local version exists for constraints:\n",constraints,"and variables:",variables)
        else:
            print(esgf.ds_count(),"instances were found on ESGF and ",outputs.count()," on the local database")
            if sys.version_info < ( 3, 0 ):
                request=raw_input("Do you want to proceed with comparison (Y) or write current results (N) ? Y/N \n")
            else:
                request=input("Do you want to proceed with comparison (Y) or write current results (N) ? Y/N \n")
            if request == "Y":
                esgf_results, db_results=compare_instances(cmip5.session, esgf_results, db_results, orig_args.keys(), admin)

    # build table to summarise results
                urls,dataset_info=new_files(esgf_results)
                upd_urls,up_dataset_info=update_files(db_results,esgf_results)
                if upd_urls!=[]:
                    user_date="_".join([os.environ['USER'],datetime.now().strftime("%Y%m%dT%H%M")+".txt"])
                    outfile="update_"+user_date
                    fout=open(outfile,"w")
                    ds_info=open(outdir+"up-dsinfo_"+user_date,'w')
                    for line in up_dataset_info:
                        ds_info.write(line)
                    ds_info.close()
                    print("These are files to update:\n")
                    for s in upd_urls:
                        print(s.split("'")[0])
                        fout.writelines("'" +s + "'\n")
                    fout.close()
                if urls!=[]:
                    user_date="_".join([os.environ['USER'],datetime.now().strftime("%Y%m%dT%H%M")+".txt"])
                    outfile="request_"+user_date
                    fout=open(outfile,"w")
                    ds_info=open(outdir+"dsinfo_"+user_date,'w')
                    for line in dataset_info:
                        ds_info.write(line)
                    ds_info.close()
                    print("These are new files to download:\n")
                    for s in urls:
                        print(s.split("'")[0])
                        fout.writelines("'" +s + "'\n")
                    fout.close()
                    if sys.version_info < ( 3, 0 ):
                        request2=raw_input("submit a request to download these files? Y/N \n")
                    else:
                        request2=input("submit a request to download these files? Y/N \n")
                    if request2 == "Y": os.system ("cp %s %s" % (outfile, outdir+outfile)) 
        for var in variables:
            remote=[ds for ds in esgf_results if ds['variable']==var]
            local=[v for v in db_results if v.variable.__dict__['variable']==var]
            if remote != [] or local != []:
                matrix = result_matrix(matrix,orig_args['experiment'],var,remote,local)
    #write a table to summarise comparison results for each experiment in csv file
    if matrix:
        for exp in kwargs['experiment']:
            write_table(matrix,exp)

if __name__ == '__main__':
    # check python version and then call main()
    if sys.version_info < ( 2, 7):
        # python too old, kill the script
        sys.exit("This script requires Python 2.7 or newer!")

    main()
