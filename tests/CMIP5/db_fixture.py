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

import pytest
from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.Model import *
from sqlalchemy.orm.exc import NoResultFound

def insert_unique(db, klass, **kwargs):
    """
    Insert an item into the DB if it can't be found
    """
    try:
        value = db.query(klass).filter_by(**kwargs).one()
    except NoResultFound:
        value = klass(**kwargs)
        db.add(value)
    return value

def add_item(db, variable, mip, model, experiment, ensemble, path, version):
    """
    Add a new test item to the DB
    """
    instance = insert_unique(db, Instance,
            variable   = variable,
            mip        = mip,
            model      = model,
            experiment = experiment,
            ensemble   = ensemble)

    version = insert_unique(db, Version,
            instance_id = instance.id,
            path        = path,
            version     = version)

@pytest.fixture(scope="module")
def session(request, tmpdir_factory):
    session = CMIP5.connect('sqlite:///:memory:')

    dira = tmpdir_factory.mktemp('a')
    dirb = tmpdir_factory.mktemp('b')

    # Create some example entries
    db = session.session
    add_item(db,
        path       = dira.strpath,
        variable   = 'a',
        mip        = 'b',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        version    = 'v01')
    add_item(db,
        path       = dira.strpath,
        variable   = 'a',
        mip        = 'b',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        version    = 'v02')
    add_item(db,
        path       = dirb.strpath,
        variable   = 'f',
        mip        = 'g',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        version    = 'v01')
    db.commit()

    # Close the session
    def fin():
        db.close()
    request.addfinalizer(fin)

    return session
