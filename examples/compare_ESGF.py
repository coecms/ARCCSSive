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
from ARCCSSive.CMIP5.other_functions import *
from collections import defaultdict
import argparse
from datetime import datetime 
import sys
from multiprocessing import Pool

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")

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
    return vars(parser.parse_args())


def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    admin = kwargs.pop("admin")
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
    return newkwargs,admin


def format_cell(v_obj,exp):
    ''' return a formatted cell value for one combination of var_mip and mod_ens '''
    value=v_obj.version
    if exp[0:6]=='decadal': value=exp[7:] + " " + v_obj.version
    if value[0:2]=="ve": value+=" (estimate) "
    if value=="NA": value="version not defined"
    if v_obj.is_latest: 
        value += " latest on" + v_obj.checked_on
    if v_obj.to_update: value += " to update "
    return value + " | "

def result_matrix(matrix,constraints,remote,local):
    ''' Build a matrix of the results to output to csv table '''
    exp=constraints['experiment']
    var=constraints['variable']
    # for each var_mip retrieve_info create a dict{var_mip:[[(mod1,ens1), details list][(mod1,ens2), details list],[..]]}
    # they are added to exp_dict and each key will be column header, (mod1,ens1) will indicate row and details will be cell value
    exp_dict=matrix[exp]
    cell_value=defaultdict(str)
    for v in local:
        cell_value[(v.variable.mip,(v.variable.model,v.variable.ensemble))]+= format_cell(v,exp)
    for ds in remote:
        if ds['same_as']==[]:
            inst=get_instance(ds['dataset_id'])
            if exp == 'decadal':
                cell_value[(inst['mip'],(inst['model'],inst['ensemble']))]+= inst['experiment'][7:] + " " 
            cell_value[(inst['mip'],(inst['model'],inst['ensemble']))]+= inst['version'] + " latest new | " 
            
    for k,val in cell_value.items():
        exp_dict[(var,k[0])].append([k[1],val])
    matrix[exp]=exp_dict
    return matrix 


def write_table(matrix,exp):
    ''' write a csv table to summarise search
        argument matrix:
        argument exp: 
    '''
    # length of dictionary matrix[exp] is number of var_mip columns
    # maximum length of list in each dict inside matrix[exp] is number of mod/ens rows
    emat = matrix[exp]
    klist = emat.keys()
    # open/create a csv file for each experiment
    try:
        csv = open(exp+".csv","w")
    except:
        print( "Can not open file " + exp + ".csv")
    csv.write(" model_ensemble/variable," + ",".join(["_".join(x) for x in klist]) + "\n")
      # pre-fill all values with "NP", leave 1 column and 1 row for headers
      # write first two columns with all (mod,ens) pairs
    col1= [emat[var][i][0] for var in klist for i in range(len(emat[var])) ]
    col1 = list(set(col1))
    col1_sort=sorted(col1)
    # write first column with mod_ens combinations & save row indexes in dict where keys are (mod,ens) combination
    for modens in col1_sort:
        csv.write(modens[0] + "_" + modens[1])
        for var in klist:
            line = [item[1].replace(", " , " (")   for item in emat[var] if item[0] == modens]
            if len(line) > 0:
                csv.write(", " +  " ".join(line) )
            else:
                csv.write(",NP")
        csv.write("\n")
    csv.close()
    print( "Data written in table for experiment: ",exp)
    return


def new_files(remote,var):
    ''' return urls of new files to download '''
    urls=[]
    dataset_info=[]
    # this return too many we need to do it variable by variable
    for ind,ds in enumerate(remote):
        if 'same_as' not in ds.keys(): continue 
        if ds['same_as']==[]:
            inst=get_instance(ds['dataset_id'])
            ctype=ds['checksum_type']
            # found dataset local path from download url, replace thredds with /g/data1/ua6/unof...
            first=ds['files'][0]
            path="/".join(first.download_url.split("/")[1:-1])
            ds_string=",".join([var,inst['mip'],inst['model'],inst['experiment'],
                               inst['ensemble'],inst['realm'],inst['version'],
                               "/g/data1/ua6/unofficial-ESG-replica/tmp/tree/"+path])+"\n"
            dataset_info.append(ds_string)
            for f in ds['files']:
                if ctype is None: 
                    urls.append("' '".join([f.filename,f.download_url,"None","None"]))
                    dataset_info.append(",".join([f.filename,f.tracking_id,"None","None"])+"\n")
                else:
                    urls.append("' '".join([f.filename,f.download_url,ctype.upper(),f.checksum]))
                    dataset_info.append(",".join([f.filename,f.tracking_id,ctype.lower(),f.checksum])+"\n")
    return urls,dataset_info


def retrieve_ds(ds):
    ''' Retrieve info from a remote dataset object '''
    files, checksums, tracking_ids = [],[],[]
    for f in ds.files(): 
        if f.get_attribute('variable')[0]== constraints['variable']:
            files.append(f)
            checksums.append(f.checksum)
            tracking_ids.append(f.tracking_id)
            if ds.chksum_type() is None:
                chksum_type=None
            elif "256" in ds.chksum_type():
                chksum_type="sha256"
            else:
                chksum_type="md5"
    ds_info= {'version': "v" + ds.get_attribute('version'), 
        'files':files, 'tracking_ids': tracking_ids, 
        'checksum_type': chksum_type, 'checksums': checksums,
        'dataset_id':ds.dataset_id }
    return ds_info


# assign constraints from input
kwargs,admin=assign_constraints()
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
    orig_args=constraints.copy()
# search on local DB, return instance_ids
    if constraints['experiment']=='decadal':
        exp0=constraints.pop('experiment')
        outputs=cmip5.outputs(**constraints).filter(Instance.experiment.like(exp0))
    else:
        outputs=cmip5.outputs(**constraints)
# loop through returned Instance objects
    db_results=[v for o in outputs for v in o.versions]
# search in ESGF database
# you can use the key 'distrib'=False to search only one node 
# you can use the key 'node' to pass a different node url from default pcmdi
# for more info look at pyesgf module documentation
    esgfargs=constraints
    if 'mip' in constraints.keys():
        esgfargs['cmor_table']=esgfargs.pop('mip')
    if 'exp0' in locals():
        esgfargs['query']=exp0+"%"
    esgfargs['replica']=False
    esgf.search_node(**esgfargs)
    print("Found ",esgf.ds_count(),"simulations for constraints")
# loop returned DatasetResult objects
# using multiprocessing Pool to parallelise process_file
# using 8 here as it is the number ov VCPU on VDI
    if esgf.ds_count()>=1:
        results=esgf.get_ds()
        async_results = Pool(1).map_async(retrieve_ds, results)
        for ds_info in async_results.get():
            esgf_results.append(ds_info)

# append to results list of version dictionaries containing useful info 
# NB search should return only one latest, not replica version if any
        
# compare local to remote info
    print("Finished to retrieve remote data")
    if esgf_results==[]:
        if db_results!=[]:
            print("Found local version but none is currently available on ESGF nodes for constraints:\n",constraints)
        else: 
            print("Nothing currently available on ESGF nodes and no local version exists for constraints:\n",constraints)
    else:
        print(esgf.ds_count(),"instances were found on ESGF and ",outputs.count()," on the local database")
        if sys.version_info < ( 3, 0 ):
            request=raw_input("Do you want to proceed with comparison or print current results? Y/N \n")
        else:
            request=input("Do you want to proceed with comparison or print current results? Y/N \n")
        if request == "Y":
            esgf_results, db_results=compare_instances(cmip5.session, esgf_results, db_results, orig_args.keys(), admin)

# build table to summarise results
    urls,dataset_info=new_files(esgf_results,constraints['variable'])
    if urls!=[]:
        user_date="_".join([os.environ['USER'],datetime.now().strftime("%Y%m%dT%H%M")+".txt"])
        outfile="request_"+user_date
        fout=open(outfile,"w")
        ds_info=open(outdir+"new-ds-info_"+user_date,'w')
        for line in dataset_info:
            ds_info.write(line)
        ds_info.close()
        print("These are new files to download:\n")
        for s in urls:
            print(s.split("'")[0])
            fout.writelines("'" +s + "'\n")
        fout.close()
        if sys.version_info < ( 3, 0 ):
            request=raw_input("submit a request to download these files? Y/N \n")
        else:
            request=input("submit a request to download these files? Y/N \n")
        if request == "Y": os.system ("cp %s %s" % (outfile, outdir+outfile)) 
    if esgf_results != [] or db_results != []:
        matrix = result_matrix(matrix,orig_args,esgf_results,db_results)
#write a table to summarise comparison results for each experiment in csv file
if matrix:
    for exp in kwargs['experiment']:
        write_table(matrix,exp)
