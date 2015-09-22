#!/usr/bin/env python
"""
Copyright 2015 ARC Centre of Excellence for Climate Systems Science

author: Scott Wales <scott.wales@unimelb.edu.au>

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

from ARCCSSive import CMIP5
from ARCCSSive.CMIP5 import insert
from ARCCSSive.CMIP5.Model import File
import pytest

@pytest.fixture
def db():
    session = CMIP5.DB.connect()
    CMIP5.insert.insert_path('CMIP5/output1/INM/inmcm4/esmHistorical/day/land/day/r1i1p1/mrro/1/'+
        'mrro_day_inmcm4_esmHistorical_r1i1p1_19800101-19891231.nc')
    CMIP5.insert.insert_path('CMIP5/output1/INM/inmcm4/esmHistorical/day/land/day/r1i1p1/mrro/1/'+
        'mrro_day_inmcm4_esmHistorical_r1i1p1_19900101-19991231.nc')
    return session

def test_CMIP5_query(db):
    results = db.query()
    assert results.count() == 1

    assert results.filter_by(institute='INM').count() == 1
    assert results.filter_by(institute='CSIRO').count() == 0

    assert len(results[0].filenames()) == 2

