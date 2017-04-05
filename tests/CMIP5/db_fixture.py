#!/usr/bin/env python
"""
file:   tests/CMIP5/db_fixture.py
author: Scott Wales <scott.wales@unimelb.edu.au>

Copyright 2015 ARC Centre of Excellence for Climate Systems Science

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

import pytest
from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.Model import *
from sqlalchemy.orm.exc import NoResultFound
from datetime import date

def insert_unique(db, klass, **kwargs):
    """
    Insert an item into the DB if it can't be found
    """
    try:
        value = db.query(klass).filter_by(**kwargs).one()
    except NoResultFound:
        value = klass(**kwargs)
        db.add(value)
        db.commit()
    return value

def retrieve_item(db, klass, **kwargs):
    """
    Retrieve an item into the DB if it can be found
    """
    try:
        value = db.query(klass).filter_by(**kwargs).one()
    except NoResultFound:
        print( "Cannot find fixture with ", kwargs)
    return value

def add_instance_item(db, variable, mip, model, experiment, ensemble, realm):
    """
    Add a new test instance item to the DB
    """
    instance = insert_unique(db, Instance,
            variable   = variable,
            mip        = mip,
            model      = model,
            experiment = experiment,
            ensemble   = ensemble,
            realm      = realm)
    return instance.id

def add_version_item(db, instance_id, path, is_latest, checked_on, to_update, dataset_id, version):
#def add_version_item(db, **kwargs):
    """
    Add a new test version item to the DB
    """
    #version = insert_unique(db, Version,**kwargs)
    version = insert_unique(db, Version,
            instance_id = instance_id,
            path        = path,
            is_latest   = is_latest,
            checked_on  = checked_on,
            to_update  = to_update,
            dataset_id  = dataset_id,
            version     = version)
    return version.id

def add_warning_item(db, version_id, warning, added_by, added_on):
    """
    Add a new test warning item to the DB
    """

    warning = insert_unique(db, VersionWarning,
            version_id = version_id,
            warning     = warning,
            added_on    = added_on,
            added_by    = added_by)

def add_file_item(db, version_id, filename, md5, sha256):
    """
    Add a new test file item to the DB
    """

    afile   = insert_unique(db, VersionFile,
            version_id = version_id,
            filename     = filename,
            md5          = md5,
            sha256       = sha256)

@pytest.fixture(scope="module")
def session(request, tmpdir_factory):
    session = CMIP5.connect('sqlite:///:memory:')

    dira = tmpdir_factory.mktemp('a')
    dirb = tmpdir_factory.mktemp('b')

    # Create some example entries
    db = session.session
    added_on=date.today()
    inst1_id = add_instance_item(db,
        variable   = 'a',
        mip        = '6hrLev',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        realm      = 'realm')
    v11_id = add_version_item(db,
        instance_id = inst1_id,
        path        = dira.strpath,
        is_latest   = False,
        checked_on  = added_on,
        to_update  = False,
        dataset_id  = 'someid',
        version     = 'v20111201')
    v12_id = add_version_item(db,
        instance_id = inst1_id,
        path        = dira.strpath,
        is_latest   = False,
        checked_on  = added_on,
        to_update  = False,
        dataset_id  = 'someid',
        version     = 'v20120101')
    v13_id = add_version_item(db,
        instance_id = inst1_id,
        path        = dira.strpath,
        is_latest   = False,
        checked_on  = added_on,
        to_update  = False,
        dataset_id  = 'someid',
        version     = 'NA')

    inst2_id = add_instance_item(db,
        variable   = 'f',
        mip        = 'cfMon',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        realm      = 'realm')
    v21_id = add_version_item(db,
        instance_id = inst2_id,
        path        = dirb.strpath,
        is_latest   = True,
        checked_on  = added_on,
        to_update  = False,
        dataset_id  = 'someid',
        version     = 'v20111201')
    v22_id = add_version_item(db,
        instance_id = inst2_id,
        path        = dirb.strpath,
        is_latest   = False,
        checked_on  = added_on,
        to_update  = False,
        dataset_id  = 'someid',
        version     = 'v20120101')
    add_warning_item(db,
        version_id    = v11_id,
        warning    = 'Test warning for inst1 v20111201',
        added_by    = 'someone@example.com',
        added_on    = added_on)
    add_warning_item(db,
        version_id    = v12_id,
        warning    = 'Test warning for inst1 v20120101',
        added_by   = 'someone@example.com',
        added_on   = added_on)
    add_file_item(db,
        version_id    = v22_id,
        filename   = 'Somefilename',
        md5        = 'Somemd5',
        sha256     = 'Somesha256')
    add_file_item(db,
        version_id    = v22_id,
        filename   = 'Anotherfilename',
        md5        = 'Anothermd5',
        sha256     = 'Anothersha256')
    add_warning_item(db,
        version_id    = v21_id,
        warning    = 'Test warning for inst2 v20111201',
        added_by    = 'anyone@example.com',
        added_on    = added_on)

    inst = add_instance_item(db,
        variable   = 'tas',
        mip        = 'Amon',
        model      = 'ACCESS1-3',
        experiment = 'rcp45',
        ensemble   = 'r1i1p1',
        realm      = 'realm')
    vers = add_version_item(db,
        instance_id = inst,
        path        = dirb.strpath,
        is_latest   = False,
        checked_on  = added_on,
        to_update   = False,
        dataset_id  = 'someid',
        version     = 'v20130507')
    add_file_item(db,
        version_id    = vers,
        filename   = 'example.nc',
        md5        = None,
        sha256     = None)
# add more instances to test unique function
    inst0 = add_instance_item(db,
        variable   = 'tas',
        mip        = 'Amon',
        model      = 'ACCESS1-3',
        experiment = 'rcp26',
        ensemble   = 'r1i1p1',
        realm      = 'realm')
    inst0 = add_instance_item(db,
        variable   = 'a',
        mip        = 'Amon',
        model      = 'MIROC5',
        experiment = 'rcp26',
        ensemble   = 'r1i1p1',
        realm      = 'realm')
    inst0 = add_instance_item(db,
        variable   = 'a',
        mip        = '6hrLev',
        model      = 'MIROC5',
        experiment = 'rcp45',
        ensemble   = 'r2i1p1',
        realm      = 'realm')
    inst0 = add_instance_item(db,
        variable   = 'tas',
        mip        = 'cfMon',
        model      = 'MIROC5',
        experiment = 'rcp45',
        ensemble   = 'r2i1p1',
        realm      = 'realm')
    db.commit()

    # Close the session
    def fin():
        db.close()
    request.addfinalizer(fin)

    return session
