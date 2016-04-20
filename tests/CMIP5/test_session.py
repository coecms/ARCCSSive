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
    assert session.models() == ['ACCESS1-3', 'c']

def test_experiments(session):
    assert session.experiments() == ['d', 'rcp45']

def test_variables(session):
    assert session.variables() == ['a', 'f', 'tas']

def test_mips(session):
    assert session.mips() == ['6hrLev', 'Amon', 'cfMon']

def test_warnings(session):
    outs =session.outputs()
    for o in outs:
       for v in o.versions:
          for w in v.warnings:
             if v.version == u'v02': 
                my_regex=re.compile('Test warning for inst\d v02')
                assert my_regex.match(w.warning) is not None
               # assert w.warning == r'Test warning for inst{d1} v02'    
                assert w.added_by == u'someone@example.com'

def test_files(session):
    outs =session.outputs()
    for o in outs:
       for v in o.versions:
          for f in v.files:
             if v.version == u'v02': 
                assert f.filename in [u'Somefilename',u'Anotherfilename']    
                assert f.md5 in [u'Somemd5',u'Anothermd5']
                assert f.sha256 in [u'Somesha256', u'Anothersha256']

def test_all(session):
    v = session.outputs()

    assert v.count() == 3

    assert v[0].variable == u'a'
    assert len(v[0].versions) == 2
    assert v[0].versions[0].version == u'v01'
    assert v[0].versions[1].version == u'v02'
    
    assert v[1].variable == u'f'

def test_query_outputs(session):
    vars = session.outputs(mip = 'cfMon')
    assert vars.count() == 1
    assert vars[0].mip == 'cfMon'

def test_filenames(session):
    outs = session.outputs()
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

