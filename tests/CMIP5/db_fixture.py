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
from ARCCSSive.CMIP5.DB import update
from ARCCSSive.CMIP5.Model import Latest

@pytest.fixture(scope="module")
def session(request, tmpdir_factory):
    session = CMIP5.connect('sqlite:///:memory:')

    dira = tmpdir_factory.mktemp('a')
    dirb = tmpdir_factory.mktemp('b')

    # Create some example entries
    db = session.session
    db.add(Latest(
        path       = dira.strpath,
        variable   = 'a',
        mip        = 'b',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        version    = 'v01'))
    db.add(Latest(
        path       = dira.strpath,
        variable   = 'a',
        mip        = 'b',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        version    = 'v02'))
    db.add(Latest(
        path       = dirb.strpath,
        variable   = 'f',
        mip        = 'g',
        model      = 'c',
        experiment = 'd',
        ensemble   = 'e',
        version    = 'v01'))
    db.commit()

    # Update other tables
    CMIP5.DB.update(db)

    # Close the session
    def fin():
        db.close()
    request.addfinalizer(fin)

    return session
