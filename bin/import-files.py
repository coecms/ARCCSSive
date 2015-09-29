#!/usr/bin/env python
"""
file:   bin/import-files.py
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
from ARCCSSive.CMIP5.Model import Version, File, Variable

import os
import glob

cmip = CMIP5.connect('sqlite:///cmip5.db')

# Import files not present in database
for version in cmip.query(Version).filter(~Version.id.in_(cmip.query(File.id))):
    g = version.path + '/' +  \
        version.variable.variable + '_' + \
        version.variable.mip + '_' + \
        version.variable.model + '_' + \
        version.variable.experiment + '_' + \
        version.variable.ensemble + '_' + \
        '*.nc'
    for path in glob.glob(g):
        print path
        f = File(version.id, path)
        cmip.session.add(f)
    cmip.session.commit()
