#!/usr/bin/env python
"""
file:   test_filequery.py
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

from ARCCSSive import CMIP5
import pytest


@pytest.fixture
def cmip5():
    session = CMIP5.connect('sqlite:///cmip5.db')
    return session

def test_filter_files(cmip5):
    hist_files = cmip5.filter_files(
            variable   = 'tas',
            mip        = 'Amon',
            experiment = 'historicalNat',
            startYear   = 1980,
            )

    assert hist_files.count() > 0

    rcp_files = cmip5.filter_files(
            variable   = 'tas',
            mip        = 'Amon',
            experiment = 'rcp45',
            )
    assert rcp_files.count() > 0
