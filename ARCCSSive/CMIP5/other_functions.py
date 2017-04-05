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
    if "|esgfcog" in bits[8]:
        return {'model': bits[3],'experiment': bits[4],'realm':bits[6],
            'ensemble':bits[7],'version':bits[8].split("|")[0]}
    return {'model': bits[3],'experiment': bits[4],'realm':bits[6],'mip':bits[7],
            'ensemble':bits[8],'version':bits[9].split("|")[0]}

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
    keys=['variable','mip','model','experiment','ensemble','realm','version','dataset_id','path','cks_type','files']
    file_keys=['filename','tracking_id','checksum']
    f=open(flist,'r')
    inst_list=[]
    file_list=[]
    lines=f.readlines()
    values=lines[0].replace("\n","").split(',')
    ds_dict=dict(zip(keys[:-1], values))
    for line in lines[1:]:
        values=line.replace("\n","").split(',')
        if len(values)<5:
            file_list.append(dict(zip(file_keys, values)))
        else:
            ds_dict['files']=file_list
            inst_list.append(ds_dict)
            file_list=[]
            ds_dict=dict(zip(keys[:-1], values))
    ds_dict['files']=file_list
    inst_list.append(ds_dict)
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

def get_trackid(fpath):
    ''' Return trackid from netcdf file '''
    # open netcdf file
    try:
        f = cdms2.open(fpath,'r')
    except:
        print("INVALID NETCDF,%s" % fpath)
        return "INVALID" 
    # read tracking id attribute
    try:
        trackid=f.tracking_id
    except:
        trackid=None
    # close file
    f.close()
    return trackid

def list_drs_versions(path):
    ''' Returns matching string if found in directory structure '''
    return [x.split("/")[-1] for x in glob.glob(path+"/v*")]

def list_drs_files(path):
    ''' Returns matching string if found in directory structure '''
    return [x.split("/")[-1] for x in glob.glob(path+"/*.nc")]
 
def get_mip(filename):
    ''' Returns mip for instance 
        input: filename 
    '''
    dummy = file_details(filename)
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
      
def unique(outputs,column_name):
    ''' Return all distinct values for selected column and search results '''
    column=getattr(Instance,column_name)
    outs= outputs.distinct(column).group_by(column).all()
    return [o.__getattribute__(column_name) for o in outs]
      
     
# this should be taken by setting environment variable DRSTREE
# define root cirectory for drstree and /tmp/tree
try:
    drstree = os.environ['DRSTREE']
except KeyError:
    drstree = "/g/data1/ua6/DRSv2/CMIP5/GCM/"
tmptree="/g/data1/ua6/unofficial-ESG-replica/tmp/tree/"

# define date string for current date
today = date.today().strftime('%Y-%m-%d')

