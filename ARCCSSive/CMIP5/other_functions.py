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

today = date.today().strftime('%d/%m/%Y')
#from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile, VersionWarning


#def combine_constraints(args):
#    return set(itertools.product(*args))

def combine_constraints(**kwargs):
   ''' Return a set of dictionaries, one for each constraints combination '''
   return (dict(itertools.izip(kwargs, x)) for x in itertools.product(*kwargs.itervalues()))

def join_varmip(var0,mip0):
    ''' List all combinations of selected variables and mips '''
    comb = ["_".join(x) for x in itertools.product(*[var0,mip0])]
    print(comb)
    return comb

def compare_instances(remote,local):
    ''' Compare remote and local search results they're both a list of dictionaries
        :argument remote: each dict has keys version, files (objs), filenames, tracking_ids, dataset_id 
        :argument local: version, files (objs), filenames, tracking_ids, dataset_id
        :return: remote, local with updated dictionaries
    '''
    local_versions=[x['version'] for x in local]
    for ind,ds in enumerate(remote):
        indices = [i for i,x in enumerate(local_versions) if x == ds['version']]
        for i in range(len(local)):
            local[i]['checked_on'] = today
            # if version same as latest on esgf 
            if i in indices:
               local[i]['dataset_id'] = remote['dataset_id']
               local[i]['is_latest'] = True
               extra = compare_tracking_ids(ds['tracking_ids'],local[i]['tracking_ids'])
               if extra==[]:
                  local[i]['to_update'] = False
               else:
                  local[i]['to_update'] = True
            # if version undefined 
            elif local[i]['version'] in ['NA',r've\d*']:
               extra = compare_tracking_ids(ds['tracking_ids'],local[i]['tracking_ids'])
               if extra==[]:
                  local[i]['version'] = remote['version']
                  local[i]['dataset_id'] = remote['dataset_id']
                  local[i]['is_latest'] = True
                  local[i]['to_update'] = False
            # if version different or undefined but one or more tracking_ids are different assume different version from latest
            else:
                  local[i]['is_latest'] = False
        if len(local)==0 or sum( local[i]['is_latest'] for i in range(len(local)) ) == 0 : 
           remote[ind]['new']=True 
        else:
           remote[ind]['new']=False 
    return remote, local
           
            

def compare_tracking_ids(remote_ids,local_ids):
    ''' Compare the lists of the tracking_ids from a remote and a loca version of a dataset
        :argument remote_ids: list of remote tracking_ids  
        :argument local_ids: list of local tracking_ids  
        :return: result set 
    '''
    return set(remote_ids).difference(local_ids)
    
