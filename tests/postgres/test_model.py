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

from ARCCSSive.model import *
from ARCCSSive.model.cmip5 import *
from ARCCSSive.db import connect, Session

import pytest
import psycopg2
import getpass
import six

def test_path(session):
    q = session.query(base.Path).limit(5)
    assert q.count() == 5

def test_metadata(session):
    q = session.query(base.Metadata).limit(5)
    assert q.count() == 5

def test_cf(session):
    q = session.query(cfnetcdf.File).limit(5)
    assert q.count() == 5

    q = q.first()
    assert len(q.variables) > 0

    assert isinstance(q.md5, six.string_types)
    assert isinstance(q.sha256, six.string_types)

def test_version_override(session):
    value = 'v99999999'
    q = (session.query(Version)
            .outerjoin(VersionOverride)
            .filter(VersionOverride.version_id == None)
            .first())
    o = VersionOverride(version_number = value)
    q.override = o
    session.add(q)
    session.commit()
    assert q.version_number == value
    session.delete(o)
    session.commit()
    assert q.version_number != value

def test_timeseries(session):
    q = session.query(Dataset).first()
    l = q.latest_version
    vs = l.variables
    files = [f.path for v in vs for f in v.files]

def test_dataset_relationships(session):
    # Grab a file in the test data
    f = session.query(cmip5.File).first()

    # Does the file's dataset point back to the file?
    d = f.dataset
    assert d is not None
    assert isinstance(d.institute_id, six.string_types)
    assert f in d.files
    assert f.version in d.versions
    assert f.timeseries in d.variables

def test_version_relationships(session):
    # Grab a file in the test data with a defined version
    f = session.query(cmip5.File).join(cmip5_attributes_links).join(Version).filter(Version.version_number != None).first()
    # How about the version?
    v = f.version
    assert v is not None
    assert isinstance(v.version_number, six.string_types)
    assert f in v.files
    assert v.dataset == f.dataset
    assert f.timeseries in v.variables

def test_timeseries_relationships(session):
    # Grab a file in the test data
    f = session.query(cmip5.File).first()
    # The timeseries has multiple files with the same variables, dataset and version
    t = f.timeseries
    assert t is not None
    assert t.dataset == f.dataset
    assert t.version == f.version
    assert f in t.files
    # assert f.variables[0].name in t.variable_list

def test_timeseries_query(session):
    t = (session.query(Timeseries).join(Dataset)
            .filter_by(institute_id = 'CSIRO-QCCCE')
            .filter(Timeseries.variable_list.any('tasmax'))
            .first())
    assert t is not None
    assert t.dataset.institute_id == 'CSIRO-QCCCE'
    assert isinstance(t.files[0], cfnetcdf.File)
    assert t.files[0].filename is not None

def test_file(session):
    t = session.query(cfnetcdf.File).first()
    assert t.filename is not None
    assert isinstance(t.filename, six.string_types)

    t = session.query(cmip5.File).first()
    assert t.filename is not None
    assert isinstance(t.filename, six.string_types)


