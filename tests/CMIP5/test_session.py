#!/usr/bin/env python
"""
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

from ARCCSSive.CMIP5.Model import *
import re
from .db_fixture import session
from datetime import date
from ARCCSSive.model import cmip5

# Tests for the basic list queries

def test_models(session):
    assert session.models().sort() == ['ACCESS1-3', 'c', 'MIROC5'].sort()

def test_experiments(session):
    assert session.experiments().sort() == ['d', 'rcp45', 'rcp26'].sort()

def test_variables(session):
    assert session.variables().sort() == ['a', 'f', 'tas'].sort()

def test_mips(session):
    assert session.mips().sort() == ['6hrLev', 'Amon', 'cfMon'].sort()

def test_warnings(session):
    # Get an item
    out = session.outputs().first()
    assert out is not None

    version = out.latest()[0]

    version.warnings.append(VersionWarning(
        warning='Test warning',
        added_by='Test user',
        added_on=date.today()))

    assert len(version.warnings) > 0
    assert version.warnings[-1].warning == 'Test warning'
    assert version.warnings[-1].added_by == 'Test user'

def test_files(session):
    # Get an item
    out = session.outputs().first()
    assert out is not None

    version = out.latest()[0]
    assert version is not None

    f = version.files[0]
    assert isinstance(f, cmip5.File)

    assert f.filename is not None
    assert f.md5 is not None
    assert f.sha256 is not None

def test_all(session):
    v = session.outputs()

    assert v.count() > 0

    assert v[0].variable
    assert len(v[0].versions) > 0
    assert isinstance(v[0].versions[0], Version)

def test_query_outputs(session):
    var = session.outputs(mip = 'aero').first()
    assert var.mip == 'aero'

def test_filenames(session):
    o = session.outputs().first()
    for f in o.filenames():
        assert f

def test_to_str(session):
    """
    Can we call the function?
    """
    q = session.query(VersionWarning)
    assert str(q[0])
    q = session.query(VersionFile)
    assert str(q[0]) == q[0].filename

def test_drstree_path(session):
    """
    Can we call the function?
    """
    q = session.outputs().first()
    assert q.drstree_path() is not None
    assert re.match(r'/g/data1/ua6/DRSv2/CMIP5/.*/latest', q.drstree_path())
    # write assertion for drstree_path() function for version object if version is 'NA" should use v20110427
    assert re.match(r'/g/data1/ua6/DRSv2/CMIP5/.*/v[0-9]+', q.versions[0].drstree_path())

def test_latest(session):
    """
    Is the function returning the right latest version/s 
    """
    # write test for inst1_id has 3 versions, including 'NA' one
    # all with is_latest False, so return one with newest date
    q = session.outputs(
            dataset_id = 'c6d75f4c-793b-5bcc-28ab-1af81e4b679d',
            variable = 'tas',
            ).first()
    assert q.latest() is not None
    assert len(q.latest()) == 1
    assert q.latest()[0].version
