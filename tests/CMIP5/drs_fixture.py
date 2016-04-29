#!/usr/bin/env python
"""
Copyright 2016 ARC Centre of Excellence for Climate Systems Science

author: Paola Petrelli <paola.petrelli@utas.edu.au>

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

# create temporary drstree like directory to test drstree functions
@pytest.fixture(scope="module")
def drstree(request,tmpdir_factory):
    path = 'BNU-ESM/amip/mon/atmos/pr/r1i1p1'.split("/")
    drs_path = tmpdir_factory.mktemp(path[0], numbered=False)
    base = str(tmpdir_factory.getbasetemp())
    for i in range(1,len(path)): 
        drs_path = drs_path.mkdir( path[i])
    drs_path.mkdir('v20120304')
    drs_path.mkdir('v20150203')
    files_path=drs_path.mkdir('v20110201').__str__()
    f1 = open(files_path + '/f1.nc','w')
    f2 = open(files_path + '/f2.nc','w')
    f3 = open(files_path + '/f3.nc','w')
    f1.write("something")
    f2.write("something")
    f3.write("something")
    f1.close()
    f2.close()
    f3.close()
    return drs_path.__str__(),files_path
