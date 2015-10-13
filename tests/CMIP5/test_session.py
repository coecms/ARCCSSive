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

from db_fixture import session

# Tests for the basic list queries

def test_models(session):
    assert session.models() == [u'c']

def test_experiments(session):
    assert session.experiments() == [u'd']

def test_variables(session):
    assert session.variables() == [u'a', u'f']

def test_mips(session):
    assert session.mips() == [u'b', u'g']

def test_all(session):
    v = session.outputs()

    assert v.count() == 2

    assert v[0].variable == u'a'
    assert v[0].versions[0].version == u'v01'
    assert v[0].versions[1].version == u'v02'
    
    assert v[1].variable == u'f'

def test_query(session):
    vars = session.outputs(mip = 'g')
    assert vars.count() == 1
    assert vars[0].mip == 'g'

def test_filenames(session):
    outs = session.outputs()
    for o in outs:
        for f in o.filenames():
            assert f
