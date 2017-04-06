#!/usr/bin/env python
# Copyright 2017 ARC Centre of Excellence for Climate Systems Science
# author: Scott Wales <scott.wales@unimelb.edu.au>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

from ARCCSSive.cli import *
from ARCCSSive.CMIP5.Model import *

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

@pytest.fixture(scope="module")
def database():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    yield Session

@pytest.fixture
def session(database):
    s = database()
    yield s
    s.rollback()

def test_latest_one_version(session):
    """
    Return a version
    """
    i = Instance()
    session.add(i)

    v1 = Version(variable=i, version='v1')
    session.add(v1)

    q = session.query(Instance, Version).join(Version)
    q = filter_latest(q, session)
    assert q.count() == 1
    assert q.one()[1] == v1

def test_latest_two_version(session):
    """
    Return the greatest version
    """
    i = Instance()
    session.add(i)

    v1 = Version(variable=i, version='v1')
    session.add(v1)

    v2 = Version(variable=i, version='v2')
    session.add(v2)

    q = session.query(Instance, Version).join(Version)
    q = filter_latest(q, session)
    assert q.count() == 1
    assert q.one()[1] == v2

def test_latest_na_version(session):
    """
    Ignore 'NA' versions
    """
    i = Instance()
    session.add(i)

    v1 = Version(variable=i, version='v1')
    session.add(v1)

    v2 = Version(variable=i, version='v2')
    session.add(v2)
    
    v3 = Version(variable=i, version='NA')
    session.add(v3)

    q = session.query(Instance, Version).join(Version)
    q = filter_latest(q, session)
    assert q.count() == 1
    assert q.one()[1] == v2

def test_latest_flag_version(session):
    """
    is_latest has priority
    """
    i = Instance()
    session.add(i)

    v1 = Version(variable=i, version='v1')
    session.add(v1)

    v2 = Version(variable=i, version='v2', is_latest=True)
    session.add(v2)
    
    v3 = Version(variable=i, version='v3')
    session.add(v3)

    q = session.query(Instance, Version).join(Version)
    q = filter_latest(q, session)
    assert q.count() == 1
    assert q.one()[1] == v2
