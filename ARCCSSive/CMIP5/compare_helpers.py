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

from ARCCSSive.CMIP5.other_functions import write_log, get_instance, get_mip, compare_tracking_ids, compare_checksums, today 
from ARCCSSive.CMIP5.Model import Instance 
from ARCCSSive.CMIP5.pyesgf_functions import FileResult 
from collections import defaultdict
import argparse
from datetime import datetime 
import os


def format_cell(v_obj,exp):
    ''' return a formatted cell value for one combination of var_mip and mod_ens '''
    value=v_obj.version
    if exp[0:6]=='decadal': value=exp[7:] + " " + v_obj.version
    if value[0:2]=="ve": value+=" (estimate) "
    if value=="NA": value="version not defined"
    if v_obj.is_latest: 
        value += " latest on " + v_obj.checked_on
    if v_obj.to_update: value += " to update "
    return value + " | "

def result_matrix(matrix,exp,var,remote,local):
    ''' Build a matrix of the results to output to csv table '''
    # for each var_mip retrieve_info create a dict{var_mip:[[(mod1,ens1), details list][(mod1,ens2), details list],[..]]}
    # they are added to exp_dict and each key will be column header, (mod1,ens1) will indicate row and details will be cell value
    exp_dict=matrix[exp]
    cell_value=defaultdict(str)
    for v in local:
        cell_value[(v.variable.mip,(v.variable.model,v.variable.ensemble))]+= format_cell(v,exp)
    for ds in remote:
        if 'same_as' not in ds.keys() or ds['same_as']==[]:
            inst=get_instance(ds['dataset_id'])
            if 'mip' not in inst.keys():
                inst['mip']=get_mip(ds['files'][0].filename)
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


def new_files(remote):
    ''' return urls of new files to download '''
    urls=[]
    dataset_info=[]
    # this return too many we need to do it variable by variable
    for ind,ds in enumerate(remote):
        if 'same_as' not in ds.keys(): continue 
        if ds['same_as']==[]:
            ctype=ds['checksum_type']
            # found dataset local path from download url, replace thredds with /g/data1/ua6/unof...
            if ds['files']==[]:
                print("This dataset has no files, skip ",ds['dataset_id'])
                continue
            if ctype is None: ctype="None"
            first=ds['files'][0]
            inst=get_instance(ds['dataset_id'])
            if 'mip' not in inst.keys():
                inst['mip']=get_mip(first.filename)
            path="/".join(first.download_url.split("/")[1:-1])
            ds_string=",".join([ds['variable'],inst['mip'],inst['model'],inst['experiment'],
                               inst['ensemble'],inst['realm'],inst['version'],ds['dataset_id'],
                               "/g/data1/ua6/unofficial-ESG-replica/tmp/tree"+path,ctype])+"\n"
            dataset_info.append(ds_string)
            for f in ds['files']:
                if f.tracking_id is None: 
                    tracking_id="None"
                else:
                    tracking_id=f.tracking_id
                if ctype is "None" or f.checksum is None :
                    checksum="None"
                else:
                    checksum=f.checksum
                urls.append("' '".join([f.filename,f.download_url,ctype.upper(),checksum]))
                dataset_info.append(",".join([f.filename,tracking_id,checksum])+"\n")
    return urls,dataset_info


def update_files(local,remote):
    ''' return urls of files to update '''
    urls=[]
    dataset_info=[]
    # this return too many we need to do it variable by variable
    for ind,ds in enumerate(remote):
        if ds['files']==[]:
            print("This dataset has no files, skip ",ds['dataset_id'])
            continue
        if ds['update'] != []: 
            dsfnames=set()
            for f in ds['files']:
                dsfnames.add(f.filename)
            for vid in ds['update']:
                v=[x for x in local if x.id==vid][0]
                missing=dsfnames.difference(v.filenames())
                extra=set(v.filenames()).difference(dsfnames)
                same=dsfnames.intersection(v.filenames())
            ctype=ds['checksum_type']
            different=check_same(same,v,ds['files'],ctype) + list(missing)
            if different != []:
                inst=get_instance(ds['dataset_id'])
            # found dataset local path from download url, replace thredds with /g/data1/ua6/unof...
                if ctype is None: ctype="None"
                first=ds['files'][0]
                path="/".join(first.download_url.split("/")[1:-1])
                ds_string=",".join([ds['variable'],inst['mip'],inst['model'],inst['experiment'],
                               inst['ensemble'],inst['realm'],inst['version'],ds['dataset_id'],
                               "/g/data1/ua6/unofficial-ESG-replica/tmp/tree"+path,ctype])+"\n"
                dataset_info.append(ds_string)
                for f in ds['files']:
                    if f.filename not in different: continue
                    if ctype=="None": 
                        urls.append("' '".join([f.filename,f.download_url,"None","None"]))
                        dataset_info.append(",".join([f.filename,f.tracking_id,"None"])+"\n")
                    else:
                        urls.append("' '".join([f.filename,f.download_url,ctype.upper(),f.checksum]))
                        dataset_info.append(",".join([f.filename,f.tracking_id,f.checksum])+"\n")
    return urls,dataset_info


def check_same(same,v,dsfiles,ctype):
    ''' '''
    different=[]
    tocompare=[]
    for fname in same:
        tup0=[f for f in v.files if f.filename==fname][0]
        tup1=[f for f in dsfiles if f.filename==fname][0] 
        tocompare.append((tup0,tup1)) 
    for tup in tocompare:
        if tup[0].__dict__[ctype]!=tup[1].checksum: different.append(tup[0].filename)
    return different

def retrieve_ds(args):
    ''' Retrieve info from a remote dataset object '''
    (ds, variables) = args
    fvar=FileResult.get_variable
    allfiles=ds.files()
    #  use first file checksum to work out checksum type 
    # this is faster than calling ds.chksum_type()
    if allfiles[0].checksum is None:
        chksum_type='None'
    else:
        sumlen=len(allfiles[0].checksum)
        if sumlen==64:
            chksum_type='sha256'
        elif sumlen==32:
            chksum_type='md5'
        else:
            chksum_type='None'
    # loop through variables and save info in a dictionary each
    ds_info=[]
    for var in variables:
        files = [ f for f in allfiles if fvar(f)==var ]
        if files!=[]:
            ds_info.append( {'version': "v" + ds.get_attribute('version'), 
                             'checksum_type': chksum_type,
                             'dataset_id':ds.dataset_id,
                             'variable': var,
                             'files':files } ) 
    return ds_info


def compare_instances(db,remote,local,const_keys,admin):
    ''' Compare remote and local search results they're both a list of dictionaries
        :argument db: sqlalchemy local db session 
        :argument remote: each dict has keys version, files (objs), filenames, tracking_ids, dataset_id 
        :argument local:  list of version objects
        :argument const_keys:  list of instance attributes defined by user constraints
        :argument admin:  boolean if True user is admin and can update db directly, optherwise new info saved in log file
        :return: remote, local with updated attributes 
    '''
    global logfile
    logdir="/g/data1/ua6/unofficial-ESG-replica/tmp/pxp581/requests/"
    if not admin:
        logfile=logdir+"log_" + os.environ['USER'] + "_" + today.replace("-","") + ".txt"
        #print(logfile)
    # a list of the unique constraints defining one instance in the database which are not in user constraints
    undefined=[x for x in Instance.__table_args__[0].columns.keys() if x not in const_keys]
    # loop through all returned remote datasets
    for ind,ds in enumerate(remote):
        # loop through all local versions and choose only version with same fields as ds
        ds_instance=get_instance(ds['dataset_id'])
        ds_instance['variable']=ds['variable']
        for v in local:
            dummy=[False for key in undefined if  ds_instance[key] != v.variable.__dict__[key]]
            if False in dummy:
                continue
            v.checked_on = today
            # compare files for all cases except if version regular but different from remote 
            if v.version in [ds['version'],'NA',r'v\d',r'\d']:
                extra = compare_files(db,ds,v,admin)
                # if tracking_ids or checksums are same
                if extra==set([]):
                    v.to_update = False
                else:
                    v.to_update = True
                    if not admin:
                        ds_info=[str(x) for x in ds_instance.items()]
                        write_log(" ".join(["update"]+ds_info+[v.version,v.path,"\n"]))
            # if local dataset_id is the same as remote skip all other checks
            if v.dataset_id==ds['dataset_id']:
                v.is_latest = True
            # if version same as latest on esgf 
            elif v.version == ds['version']:
                v.dataset_id = ds['dataset_id']
                v.is_latest = True
            # if version undefined 
            elif v.version in ['NA',r'v\d*',r'\d']:
                if extra==set([]):
                    v.version = ds['version']
                    v.dataset_id = ds['dataset_id']
                    v.is_latest = True
            # if version different or undefined but one or more tracking_ids are different
            # assume different version from latest
            # NB what would happen if we fix faulty files? tracking_ids will be same but md5 different, 
            # need to set a standard warning for them
            else:
                v.is_latest = False
                if v.version > ds['version']:
                    print("Warning!!!")
                    print(" ".join(["Local version",v.version,"is more recent than the latest version",ds['version'], "found on ESGF"]))
                if v.dataset_id is None: v.dataset_id = "NA"
    # update local version on database
            if admin:
                db.commit()
            else:
                if db.dirty:
                    line=["version"]+ds_instance.values()[:-1]+[v.version,str(v.id),str(v.dataset_id),
                          str(v.is_latest),str(v.checked_on),"\n"]
                    write_log(" ".join(line))

    # add to remote dictionary list of local identical versions
        remote[ind]['same_as']=[v.id for v in local if v.dataset_id == ds['dataset_id'] 
                                and v.variable.__dict__['variable'] == ds['variable']]
        remote[ind]['update']=[]
        remote[ind]['update']=[v.id for v in local if v.to_update and v.dataset_id == ds['dataset_id']
                                and v.variable.__dict__['variable'] == ds['variable']]
    try:
        flog.close()
    except:
        pass
    return remote, local


def compare_files(db,rds,v,admin):
    ''' Compare files of remote and local version of a dataset
        :argument rds: dictionary of remote dataset object selected attributes  
        :argument v:  local version object   
        :return: result set, NB updating VerisonFiles object in databse if calculating checksums 
    '''
    extra=None
    local_files_num=len(v.files)
    # if there are no files on db for local version add them
    if local_files_num==0:
        rows=[]
        for f in v.build_filepaths():
            rows.append(dict(filename=f.split("/")[-1], version_id=v.id))
        if admin:
            add_bulk_items(db, VersionFile, rows)
        else:
            for r in rows:
                write_log("new file "+ str(r) + "\n")
        local_files_num=len(rows)
    # first compare tracking_ids if all are present in local version
    # if a file is INVALID or missing skip both tracking-id and checksums comparison
    local_ids=[x for x in v.tracking_ids() if x not in [None,""]]
    if "INVALID" in local_ids or local_files_num != len(rds['files']):
        return extra
    if len(local_ids)>0:
        rds_ids=[f.tracking_id for f in rds['files']]
        extra = compare_tracking_ids(rds_ids,local_ids)
    # calculate checksums and update local db if necessary  
    # uncomment this to check also if tracking_ids are the same
    #if extra is None or  extra==set([]):
    # if tracking_ids are not present compare checksums
    if extra is None:
        local_sums=[]
        cktype=str(rds['checksum_type']).lower()
        for f in v.files:
            try:
                cksum=f.__dict__[cktype]
            except (TypeError, KeyError):
                #print("type or key error ",cktype)
                cksum=None
            if cksum in ["",None]:
                cksum=check_hash(v.path+"/"+f.filename,cktype)
                if admin:
                    update_item(db,VersionFile,f.id,{cktype:cksum})
                else:
                    write_log(" ".join([cktype,str(f.id),cksum,"\n"]))
            local_sums.append(cksum)
        rds_sums=[f.checksum for f in rds['files']]
        extra = compare_checksums(rds_sums,local_sums)
    return extra



# functions to write logs
def write_log(line):
    ''' add str to log file, open new file if does not exist yet '''
    global flog
    try:
        flog.write(line)
    except:
        flog=open(logfile,"a")
        flog.write(line)
    return

