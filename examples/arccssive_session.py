#!/usr/bin/env python
# This check data available on ESGF and on raijin that matches constraints passed on by user and return a summary.
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

from __future__ import print_function

from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.Model import Instance 
# connect to the database
db=CMIP5.connect()
#search database instances
outputs=db.outputs(variable='tas',model='MIROC5',experiment='historical',mip='Amon',ensemble='r1i1p1')

# loop through result instance objects returned by search
for o in outputs:
    model = o.model
    print(str(model)) 
    files = o.filenames()
    print(files) 
    fpath = o.drstree_path()
    print(str(fpath))
# loops through result version objects related to instance
    for v in o.versions:
        if v.is_latest: print("latest available version on ESGF as of ",str(v.checked_on))

# search without specifying variables and then use filter to select only two
outputs=db.outputs(model='MIROC5',experiment='historical',mip='Amon',ensemble='r1i1p1')\
        .filter(Instance.variable.in_(['tas','pr']))

# loop through result instance objects returned by search
for o in outputs:
    var = o.variable
    print(str(var))
    files = o.filenames()
    print(files)
    fpath = o.drstree_path()
    print(str(fpath))
# loops through result version objects related to instance
    for v in o.versions:
# print message if version is latest on ESGF
        if v.is_latest: print("latest available version on ESGF as of ",str(v.checked_on))
# print checksum and tracking-id for first file listed
    print(str(o.versions[0].files[0].sha256))
    print(str(o.versions[0].files[0].md5))
    print(str(o.versions[0].files[0].tracking_id))
