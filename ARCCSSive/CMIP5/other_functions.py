# This collects all other helper functions 
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
import itertools
from datetime import date
import glob
import subprocess
import re
#import cdms2 to open netcdf files
from collections import defaultdict
from ARCCSSive.data import mip_dict, frq_dict 
from ARCCSSive.CMIP5.update_db_functions import add_bulk_items, update_item 
from ARCCSSive.CMIP5.Model import Instance, VersionFile


def combine_constraints(**kwargs):
    ''' Works out any possible combination given lists of constraints
        :argument dictionary, keys are fields and values are lists of values for each field
        :return: a set of dictionaries, one for each constraints combination i
    '''
    try:
        return [dict(itertools.izip(kwargs, x)) for x in itertools.product(*kwargs.itervalues())]
    except:
        return [dict(zip(kwargs, x)) for x in itertools.product(*kwargs.values())]

def join_varmip(var0,mip0):
    ''' List all combinations of selected variables and mips '''
    comb = ["_".join(x) for x in itertools.product(*[var0,mip0])]
    print(comb)
    return comb

def get_instance(dataset_id):
    ''' Break dataset_id from ESGF search in dictionary of instance attributes '''
    bits=dataset_id.split(".")
    return {'model': bits[3],'experiment': bits[4],'realm':bits[6],'mip':bits[7],'ensemble':bits[8],'version':bits[9].split("|")[0]}

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
        print(logfile)
    # a list of the unique constraints defining one instance in the database which are not in user constraints
    undefined=[x for x in Instance.__table_args__[0].columns.keys() if x not in const_keys]
    # loop through all returned remote datasets
    for ind,ds in enumerate(remote):
        # loop through all local versions
        ds_instance=get_instance(ds['dataset_id'])
        for v in local:
            dummy=[False for key in undefined if  ds_instance[key] != v.variable.__dict__[key]]
            if False in dummy:
                continue
            v.checked_on = today
            # compare files for all cases except if version regular but different from remote 
            if v.version in [ds['version'],'NA',r've\d*']:
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
            elif v.version in ['NA',r've\d*']:
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
        remote[ind]['same_as']=[v.id for v in local if v.dataset_id == ds['dataset_id']] 
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
    if v.files==[]:
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
    if "INVALID" not in local_ids or local_files_num != len(rds['files']):
        return extra
    if len(local_ids)>0:
        extra = compare_tracking_ids(rds['tracking_ids'],local_ids)
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
        extra = compare_checksums(rds['checksums'],local_sums)
    return extra 
            
def compare_tracking_ids(remote_ids,local_ids):
    ''' Compare the lists of the tracking_ids from a remote and a local version of a dataset
        :argument remote_ids: list of remote tracking_ids  
        :argument local_ids: list of local tracking_ids  
        :return: result set 
    '''
    return set(remote_ids).symmetric_difference(local_ids)
    
def compare_checksums(remote_sums,local_sums):
    ''' Compare the lists of the checksums from a remote and a local version of a dataset
        :argument remote_sums: list of remote checksums  
        :argument local_sums: list of local checksums  
        :return: result set 
    '''
    return set(remote_sums).symmetric_difference(local_sums)
    
# these functions are to manage drstree and tmp/tree directories

def list_drstree(**kwargs):
    ''' find directories matching kwargs constraints in drstree
        check if they are in database,
        if not add them to db
        return: list of matches '''
    if 'mip' in kwargs.keys(): kwargs['frequency']=frequency(kwargs['mip'])
    indir=drstree + drs_glob(**kwargs)
    return glob.glob(indir)

def list_tmpdir(flist):
    ''' this read from file list of instances on tmp/tree and return the ones matching constraints '''
# skip first line
# variable,mip_table,model,experiment,ensemble,realm,version,path
    keys=['variable','mip','model','experiment','ensemble','realm','version','path']
    f=open(flist,'r')
    inst_list=[]
    lines=f.readlines()
    for line in lines[1:]:
        values=line[:-1].split(',')
        inst_list.append(dict(zip(keys, values)))
    f.close()
    return inst_list

def list_logfile(flist):
    ''' this read from file list of instances from download log file and return the ones matching constraints '''
    keys=['variable','mip','model','experiment','ensemble','realm','version','path','cks_type','files']
    file_keys=['filename','tracking_id','checksum']
    f=open(flist,'r')
    inst_list=[]
    file_list=[]
    lines=f.readlines()
    values=lines[0].replace("\n","").split(',')
    var=values[0]
    ds_dict=dict(zip(keys[:-1], values))
    for line in lines[1:]:
        values=line.replace("\n","")[:-1].split(',')
        if values[0]!=var:
            file_list.append(dict(zip(file_keys, values)))
        else:
            ds_dict['files']=file_list
            inst_list.append(ds_dict)
            file_list=[]
            ds_dict=dict(zip(keys[:-1], values))
    f.close()
    return inst_list

def file_glob(**kwargs):
    """ Get the glob string matching the CMIP5 filename
    """
    value=defaultdict(lambda: "*")
    value.update(kwargs)
    return '%s_%s_%s_%s_%s*.nc'%(
            value['variable'],
            value[ 'mip'],
            value['model'],
            value['experiment'],
            value['ensemble'])

def drs_glob(**kwargs):
    """ Get the glob string matching the directory structure under drstree
    """
    value=defaultdict(lambda: "*")
    value.update(kwargs)
    return '%s/%s/%s/%s/%s/%s/%s'%(
        value['model'],
        value['experiment'],
        value['frequency'],
        value['realm'],
        value['variable'],
        value['ensemble'],
        value['version'])

def tree_glob(**kwargs):
    """ Get the glob string matching the directory structure under tmp/tree
    """
    value=defaultdict(lambda: "*")
    value.update(kwargs)
    return '%s/%s/%s/%s/%s/%s'%(
        value['model'],
        value['experiment'],
        value['frequency'],
        value['realm'],
        value['variable'],
        value['ensemble'])

def drs_details(path):
    ''' Split the drstree path in model, experiment, frequency, realm, variable, ensemble '''
    keys=['model','experiment', 'frequency', 'realm', 'variable','ensemble']
    values=path.replace(drstree,"").split('/')
    dummy=dict((keys[i],values[i]) for i in range(len(values)))
    return dummy.pop('frequency'), dummy

def file_details(fname):
    ''' Split the filename in variable, MIP code, model, experiment, ensemble (period is excluded) '''
    keys=['variable','mip','model','experiment','ensemble']
    values = fname.split('_')
    if len(values) >= 5:
        return dict((keys[i],values[i]) for i  in range(len(values[:-1])))
    else:
        return 

def find_version(bits,string):
    ''' Returns matching string if found in directory structure '''
    dummy = filter(lambda el: re.findall( string, el), bits)
    if len(dummy) == 0:
        return 'not_specified'
    else:
        return dummy[0]

#def get_trackid(fpath):
#    ''' Return trackid from netcdf file '''
#    # open netcdf file
#    try:
#        f = cdms2.open(fpath,'r')
#    except:
#        print("INVALID NETCDF,%s" % fpath)
#        return "INVALID" 
#    # read tracking id attribute
#    try:
#        trackid=f.tracking_id
#    except:
#        trackid=None
#    # close file
#    f.close()
#    return trackid

def list_drs_versions(path):
    ''' Returns matching string if found in directory structure '''
    return [x.split("/")[-1] for x in glob.glob(path+"/v*")]

def list_drs_files(path):
    ''' Returns matching string if found in directory structure '''
    return [x.split("/")[-1] for x in glob.glob(path+"/*.nc")]
 
def get_mip(path):
    ''' Returns mip for instance 
        input: instance path
    '''
    onefile = os.path.basename(glob.glob(path + "/latest/*.nc")[0]) 
    dummy = file_details(onefile)
    return dummy['mip']

def tree_path(drspath):
    ''' Returns the tmp/tree path for a particular instance &  version from 
        input: drstree path for one the version files
    '''
    path=os.path.realpath(drspath)
    return "/".join(path.split("/")[:-1])

def check_hash(path,hash_type):
    ''' Execute md5sum/sha256sum on file on tree and return checksum value '''
    hash_cmd="md5sum"
    if hash_type in ["SHA256","sha256"]: hash_cmd="sha256sum"
    try:
        return subprocess.check_output([hash_cmd, path]).split()[0]
    except:
        print("Warning cannot calculate ",hash_type," for file ",path)
        return ""

# functions to manage dictionaries
def assign_mips(**kwargs):
    ''' Append the cmip5 mip tables corresponding to the input frequency 
        return updates list of mips '''
    if kwargs['mip'] is None: kwargs['mip']=[]
    if kwargs['frq']:
        kwargs['mip'].extend([y for x in kwargs['frq'] for y in frq_dict[x]]) 
    return list(set([x for x in kwargs['mip']]))

def frequency(mip):
    ''' returns frequency for input mip '''
    return  mip_dict[mip]

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
      
      
     
# this should be taken by setting environment variable DRSTREE
# define root cirectory for drstree and /tmp/tree
try:
    drstree = os.environ['DRSTREE']
except KeyError:
    drstree = "/g/data1/ua6/drstree/CMIP5/GCM/"
drstree="/g/data1/ua8/cmip-download/drstree/CMIP5/GCM/"
tmptree="/g/data1/ua6/unofficial-ESG-replica/tmp/tree/"

# define date string for current date
today = date.today().strftime('%Y-%m-%d')

