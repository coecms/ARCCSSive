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

import re
from .db_fixture import session

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
    outs =session.outputs()
    for o in outs:
       for v in o.versions:
          for w in v.warnings:
             if v.version == u'v20120101': 
                my_regex=re.compile('Test warning for inst\d v20120101')
                assert my_regex.match(w.warning) is not None
               # assert w.warning == r'Test warning for inst{d1} v20120101'    
                assert w.added_by == u'someone@example.com'

def test_files(session):
    outs =session.outputs()
    for o in outs:
       for v in o.versions:
          for f in v.files:
             if v.version == u'v20120101': 
                assert f.filename in [u'Somefilename',u'Anotherfilename']    
                assert f.md5 in [u'Somemd5',u'Anothermd5']
                assert f.sha256 in [u'Somesha256', u'Anothersha256']

def test_all(session):
    v = session.outputs()

    assert v.count() == 7 

    assert v[0].variable == u'a'
    assert len(v[0].versions) ==3 
    assert v[0].versions[0].version == u'NA'
    assert v[0].versions[1].version == u'v20111201'
    assert v[0].versions[2].version == u'v20120101'
    
    assert v[1].variable == u'f'

def test_query_outputs(session):
    vars = session.outputs(mip = 'cfMon')
    assert vars.count() == 2 
    assert vars[0].mip == 'cfMon'

def test_filenames(session):
    outs = session.outputs(experiment='d')
    for o in outs:
        for f in o.filenames():
            assert f

def test_to_str(session):
    """
    Can we call the function?
    """
    from ARCCSSive.CMIP5.Model import VersionWarning, VersionFile
    q = session.query(VersionWarning)
    assert str(q[0])
    q = session.query(VersionFile)
    assert str(q[0]) == q[0].filename

def test_drstree_path(session):
    """
    Can we call the function?
    """
    q = session.outputs()
    assert q[0].drstree_path() is not None
    assert q[0].drstree_path() ==  "/g/data1/ua6/DRSv2/CMIP5/c/d/6hr/realm/e/a/latest"
    # write assertion for drstree_path() function for version object if version is 'NA" should use v20110427
    assert q[0].versions[0].drstree_path() ==  "/g/data1/ua6/DRSv2/CMIP5/c/d/6hr/realm/e/a/v20110427"
    assert q[0].versions[2].drstree_path() ==  "/g/data1/ua6/DRSv2/CMIP5/c/d/6hr/realm/e/a/v20120101"

def test_latest(session):
    """
    Is the function returning the right latest version/s 
    """
    # write test for inst1_id has 3 versions, including 'NA' one
    # all with is_latest False, so return one with newest date
    q = session.outputs()
    assert q[0].latest() is not None
    assert len(q[0].latest()) == 1
    assert q[0].latest()[0].version == 'v20120101'
    # write test for inst2_id has 2 versions, oldest one has is_latest True
    assert q[1].latest()[0].version == 'v20111201'
