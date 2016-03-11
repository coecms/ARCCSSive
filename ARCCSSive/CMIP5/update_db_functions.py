# This collects all functions to update database using SQLalchemy
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
import glob
import pickle
import subprocess
from collections import defaultdict

from sqlalchemy.orm.exc import NoResultFound
from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile, VersionWarning


def search_item(db, klass, **kwargs):
    """
    Search for item in DB, if it can't be found return False
    Otherwise returns only first item
    """
    try:
        item = db.query(klass).filter_by(**kwargs).one()
    except NoResultFound:
        return False
    return item

def insert_unique(db, klass, **kwargs):
    """
    Insert an item into the DB if it can't be found
    NB need to commit before terminating session
    Returns id, True if new item, id False if existing
    """
    item = search_item(db, klass, **kwargs)
    new=False
    if not item:
        item = klass(**kwargs)
        db.add(item)
        new=True
        #print("before",item.id)
        db.commit() 
        #print("after",item.id)
    return item, new

def update_item_old(db, klass, item_id, **kwargs):
    """
    Update an existing item into the DB 
    NB need to commit before terminating session
    """
    item=search_item(db, klass, id=item_id)
    if not item:
       print("Warning cannot update item does not exist yet") 
       return 
    for k,v in kwargs.items():
        item.k = v
    # is this correct only to update?? surely not!
    db.add(item)
    db.commit()
    return item

def add_bulk_items(db, klass, rows):
    """Batched INSERT statements via the ORM "bulk", using dictionaries.
       input: rows is a list of dictionaries """
    db.bulk_insert_mappings(klass,rows) 
    db.commit()
    return

def update_item(db, klass, item_id, **kwargs):
    '''
    '''
    db.query(klass).filter(id == item_id).update(**kwargs)
    db.commit()
    return

def commit_changes(db):
    ''' Commit changes to database '''
    return db.commit()

# these functions are to manage drstree and tmp/tree directories

def list_dir(root, **kwargs):
    ''' find directories matching kwargs constraints in drstree
        check if they are in database,
        if not add them to db
        return: list of matches '''
    if 'mip' in kwargs.keys(): kwargs['frequency']=frequency(kwargs['mip'])
    if root==drstree:
       indir=root + drs_glob(**kwargs)
    elif root==tmptree:
       indir=root + tree_glob(**kwargs)
    return glob.glob(indir)

def list_tmpdir(flist):
    ''' this read from file list of instances on tmp/tree and return the ones matching constraints '''
 # I can now read all I need including to create a drspath from ~pxp581/allcmip5data.csv
# skip first line
# variable,mip_table,model,experiment,ensemble,realm,version,path
# possibly cut \n at end use ','.split(line) to get the above list
#loop through entire drstree or a subdir by using constraints **kwargs
    keys=['variable','mip','model','experiment',
             'ensemble', 'realm', 'version', 'path']
    f=open(flist,'r')
    inst_list=[]
    lines=f.readlines()
    for line in lines[1:]:
       values=line[:-1].split(',')
       inst_list.append( {keys[i]:values[i] for i in range(len(keys))} )
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
    return '%s/%s/%s/%s/%s/%s'%(
        value['model'],
        value['experiment'],
        value['frequency'],
        value['realm'],
        value['variable'],
        value['ensemble'])

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
    ''' Execute md5sum/sha256sum on file on tree and return True,f same as in wget file '''
    hash_cmd="md5sum"
    if hash_type in ["SHA256","sha256"]: hash_cmd="sha256sum"
    try:
      return subprocess.check_output([hash_cmd, path]).split()[0]
    except:
      print("Warning cannot calculate ",hash_type," for file ",path)
      return ""

# functions to manage dictionaries
def assign_mips(*args):
    ''' Append the cmip5 mip tables corresponding to the input frequency and/or realm
        return updates list of mips '''
    if not mips: mips=[]
    if frq:
       mips.extend(frq_dict[frq]) 
    if realm:
       mips.extend(realm_dict[realm]) 
    return mips

def frequency(mip):
    ''' returns frequency for input mip '''
    return  mip_dict[mip]
     
# what ab trying a dictionary to load separately???

# this should be taken by setting environment variable DRSTREE
#drstree="/g/data1/ua6/drstree/CMIP5/GCM/"
drstree="/g/data1/ua8/cmip-download/drstree/CMIP5/GCM/"
tmptree="/g/data1/ua6/unofficial-ESG-replica/tmp/tree/"
picklefile = open("cmip_dict_pickle", 'r')
mip_dict = pickle.load(picklefile)
frq_dict = pickle.load(picklefile)
picklefile.close()
