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

import pytest
from ARCCSSive.CMIP5.other_functions import *

def test_frequency():
    assert frequency('Amon') == ['mon']

def test_constraints():
    data = {'variable': ['a','b'],
            'model': ['m1','m2','m3'],
            'experiment': ['e1','e2'],
            'field4': ['v1']}
    results = list(combine_constraints(**data))
    assert len(results)==12
    for r in results:
       assert type(r) is dict
       assert len(r)==len(data.keys())
    assert {'variable': 'b', 'model': 'm2', 'experiment': 'e1', 'field4': 'v1'} in results
    assert {'variable': 'b', 'model': 'm2', 'field4': 'v1'} not in results

def test_compare_tracking_ids():
    local=['34rth678','de45t','abc123']
    # remote ids same as local, different, empty
    remote=[['34rth678','de45t','abc123'],['34rth678','de45t'],[]]
    output=[set([]),set(['abc123']),set(local)]
    for i in range(3):
       assert compare_tracking_ids(remote[i],local)==output[i]

def test_compare_checksums():
    local=['34rth678','de45t','abc123']
    # remote checksums same as local, different, empty
    remote=[['34rth678','de45t','abc123'],['34rth678','de45t'],[]]
    output=[set([]),set(['abc123']),set(local)]
    for i in range(3):
       assert compare_checksums(remote[i],local)==output[i]

def test_assign_mips():
    data=[{'mip':['Amon','Omon'],'frq':['3hr']},{'mip':[],'frq':['3hr','fx']}] 
    output=[['Amon','Omon','3hr','3hrLev','cf3hr','cfSites'],['3hr','3hrLev','cf3hr','cfSites','fx']]
    for i in range(2):
       assert set(assign_mips(**data[i]))==set(output[i])

def test_list_drs_versions(drstree):
    assert set(list_drs_versions(drstree_path))==set(['v20120304','v20110201','v20150203'])

def test_list_drs_files(drstree):
    assert set(list_drs_files(drstree_path))==set(['f1.nc','f2.nc','f3.nc'])


# create temporary drstree like directory to test drstree functions
@pytest.fixture(scope="module")
def drstree(tmpdir_factory):
    path = 'BNU-ESM/amip/mon/atmos/pr/r1i1p1'.split("/")
    drs = tmpdir_factory.mktemp(path[0], numbered=False)
    base = str(tmpdir_factory.getbasetemp())
    print(dir(drs))
    for i in range(1,len(path)): 
        drs = drs.mkdir( path[i])
        #dir = tmpdir_factory.mktemp(dir.replace(base,"")).join(path[1])
    #    base = tmpdir_factory.getbasetemp()
    
    print(drs,base)
    v1dir = drs.mkdir('v20120304')
    v2dir = drs.mkdir('v20110201')
    v3dir = drs.mkdir('v20150203')
    f1dir = tmpdir_factory.mktemp(v2dir).join('f1.nc')
    f2dir = tmpdir_factory.mktemp(v2dir).join('f2.nc')
    f3dir = tmpdir_factory.mktemp(v2dir).join('f3.nc')
