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

from ARCCSSive.CMIP5.update_db_functions import *
from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile
from .db_fixture import session

def test_search_item(session):
    db=session.session 
    kwargs={'variable': 'a', 'mip': '6hrLev', 'model': 'c',
            'experiment': 'd', 'ensemble': 'e','realm':  'realm'}
    item = search_item(db,Instance,**kwargs)
    assert type(item.id) is int 
    assert item.id==1

def test_insert_unique(session):
    db=session.session
    kwargs={'variable': 'a2', 'mip': '6hrLev', 'model': 'c',
            'experiment': 'd', 'ensemble': 'e','realm':  'realm'}
    item, new= insert_unique(db,Instance,**kwargs)
    assert new is True
    assert type(item.id) is int 

def test_update_item(session):
    db=session.session
    item_id=2
    item = search_item(db,Version,**{'id': item_id})
    assert item.is_latest is False
    newvalues={'is_latest': True}
    update_item(db, Version, item_id, newvalues)
    item = search_item(db,Version,**{'id': item_id})
    assert item.is_latest is True

def test_add_bulk_items(session):
    db=session.session
    vid=1
    num1=session.query(VersionFile).filter(VersionFile.version_id==vid).count() 
    data=[('f1.nc','s2it4'),('f2.nc','3grt4'),('f3.nc','3abt4'),('f4.nc','3gr32')]
    rows=[dict(filename=x[0], md5=x[1], version_id=vid) for x in data]
    add_bulk_items(db,VersionFile,rows)
    num2=session.query(VersionFile).filter(VersionFile.version_id==vid).count() 
    assert num2==num1+4
