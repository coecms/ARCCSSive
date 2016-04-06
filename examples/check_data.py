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
from collections import defaultdict

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")

def parse_input():
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
    parser.add_argument('-t','--mip', type=str, nargs="*", help='CMIP5 MIP table', required=False)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    #parser.add_argument('-r','--realm', type=str, nargs="*", help='CMIP5 realm', required=False)
    parser.add_argument('-en','--ensemble', type=str, nargs="*", help='CMIP5 ensemble', required=False)
    parser.add_argument('-ve','--version', type=str, nargs="*", help='CMIP5 version', required=False)
    parser.add_argument('-o','--output', type=str, nargs=1, help='urls to download output file name', required=False)
    return vars(parser.parse_args())


def assign_constraints():
    ''' Assign default values and input to constraints '''
    kwargs = parse_input()
    frq = kwargs.pop("frequency")
    kwargs["mip"] = assign_mips(frq=frq,mip=kwargs["mip"])
    for k,v in kwargs.items():
        if v is None: kwargs.pop(k)
    return kwargs


def format_cell(v_obj):
    ''' return a formatted cell value for one combination of var_mip and mod_ens '''
    value=v_obj.version
    if value[0:2]=="ve": value+=" (estimate) "
    if value=="NA": value="version not defined"
    if v_obj.is_latest: 
       value += " latest "
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
        cell_value[(v.variable.mip,(v.variable.model,v.variable.ensemble))]+= format_cell(v)
    for ds in remote:
        if ds['same_as']==[]:
           inst=get_instance(ds['dataset_id'])
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
    # calculate ncol,nrow 
    ncol = len(klist) +2 
    nrow = max([len(emat[x]) for x in klist]) +1
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

def new_files(remote):
    ''' return urls of new files to download '''
    urls=[]
    # this return too many we need to do it variable by variable
    for ind,ds in enumerate(remote):
        if ds['same_as']==[]:
           for f in ds.files():
               urls.append(f.download_url)
    return urls


# Should actually use parse_input() function from other_functions.py to build kwargs from input
kwargs=assign_constraints()
outfile=kwargs.pop("output","download_urls.txt")
fout=open(outfile,"w")

# a list fo the standard unique constraints defining one instance in the database
# initialize dictionary of exp/matrices
matrix=defaultdict(lambda: defaultdict(list))
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
# you can use the key 'distrib'=False to search only one node 
# you can use the key 'node' to pass a different node url from default pcmdi
# for more info look at pyesgf module documentation
    orig_args=constraints
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
           print("Nothing currently available on ESGF nodes and no local version exists for constraints:\n",constraints)
    else:
       esgf_results, db_results=compare_instances(cmip5.session, esgf_results, db_results, orig_args.keys())

# build table to summarise results
    urls=new_files(esgf_results)
    if urls!=[]:
      print("These are new files to download:\n")
      print(urls)
      fout.writelines(x + "\n" for x in urls)
    matrix = result_matrix(matrix,constraints,esgf_results,db_results)
#write a table to summarise comparison results for each experiment in csv file
for exp in kwargs['experiment']:
    write_table(matrix,exp)
