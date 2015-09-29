#!/usr/bin/env python
"""
file:   bin/import.py
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

from ARCCSSive import CMIP5, latest
from ARCCSSive.CMIP5.insert import get_or_insert

# Connect to the db of latest files
input = latest.connect()

# Connect to the output db
output = CMIP5.connect('sqlite:///cmip5.db')

nread = 0
for file in input.query(latest.Model.File):
    if nread % 20 == 0:
        print file.path
    nread += 1

    version = get_or_insert(output.session,
            CMIP5.Model.Version,
            version     = file.version,
            path        = file.path)

    if version.variable_id is None:
        variable = get_or_insert(output.session,
                CMIP5.Model.Variable,
                model      = file.model,
                experiment = file.experiment,
                variable   = file.variable,
                mip        = file.mip,
                ensemble   = file.ensemble)
        version.variable_id = variable.id

