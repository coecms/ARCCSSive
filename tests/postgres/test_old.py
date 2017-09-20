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

from ARCCSSive.model.old_cmip5 import *
import six

def test_version(session):
    v = session.query(Version).filter_by(version_id = '6e33fc9d-06aa-417b-7b88-99ac519ed7fe', variable_name = 'tauvo').one()

    assert v.path == '/g/data1/ua6/unofficial-ESG-replica/tmp/tree/esgdata.gfdl.noaa.gov/thredds/fileServer/gfdl_dataroot/NOAA-GFDL/GFDL-CM3/historicalMisc/mon/ocean/Omon/r5i1p1/v20110601/tauvo'
