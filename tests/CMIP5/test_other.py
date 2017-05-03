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
from .drs_fixture import drstree
from .db_fixture import session

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
    drs_path=drstree[0]
    assert set(list_drs_versions(drs_path))==set(['v20120304','v20110201','v20150203'])

def test_list_drs_files(drstree):
    files_path=drstree[1]
    assert set(list_drs_files(files_path))==set(['f1.nc','f2.nc','f3.nc'])

def test_get_instance():
    dataset_id='cmip5.output1.MIROC.MIROC-ESM-CHEM.rcp45.mon.atmos.Amon.r3i1p1.v20150209|esgf-data1.diasjp.net'
    dataset_id_exception='cmip5.output.CCCma.CanESM2.rcp85.mon.ocean.r2i1p1.v20130331|esgfcog.cccma.ec.gc.ca'
    inst_dict={'model':'MIROC-ESM-CHEM','experiment':'rcp45','realm':'atmos','mip':'Amon',
               'ensemble':'r3i1p1','version':'v20150209'}
    inst_dict_exception={'model':'CanESM2','experiment':'rcp85','realm':'ocean', 
               'ensemble':'r2i1p1','version':'v20130331'}
    assert get_instance(dataset_id)==inst_dict
    assert get_instance(dataset_id_exception)==inst_dict_exception
    assert 'mip' not in get_instance(dataset_id_exception).keys()

def test_get_mip():
    filename='thetao_Omon_CanESM2_rcp85_r2i1p1_209101-210012.nc'
    assert get_mip(filename)=='Omon'

def test_file_details():
    filename='thetao_Omon_CanESM2_rcp85_r2i1p1_209101-210012.nc'
    assert file_details(filename)=={'variable':"thetao",'mip':"Omon",
               'model':"CanESM2",'experiment': "rcp85", 'ensemble': "r2i1p1"}

def test_unique(session):
    outs=session.outputs()
    models=['c','ACCESS1-3','MIROC5'].sort()
    ensembles=['e','r1i1p1','r2i1p1'].sort()
    experiments=['d','rcp45','rcp26'].sort()
    variables=['a','tas'].sort()
    mips=['6hrLev','cfMon','Amon'].sort()
    assert unique(outs,'model').sort() == models
    assert unique(outs,'ensemble').sort() == ensembles
    assert unique(outs,'experiment').sort() == experiments
    assert unique(outs,'variable').sort() == variables
    assert unique(outs,'mip').sort() == mips
     
