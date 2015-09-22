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

from DB import Session
from Model import Dataset, Version, File

def get_or_insert(session, klass, **kwargs):
    """Return the id of a entry

    Creates a new entry with kwargs if one doesn't exist
    """
    search = session.query(klass).filter_by(**kwargs)

    if search.count() > 1:
        raise IndexError('Too many matches')
    elif search.count() == 1:
        return search[0].id
    else:
        entry = klass(**kwargs)
        session.add(entry)
        session.commit()
        return entry.id

def insert_path(path):
    """ Insert a DRS path into the database

    Example:
        
        from ARCCSSive import CMIP5
        CMIP5.insert('CMIP5/output1/INM/inmcm4/esmHistorical/day/land/day/r1i1p1/mrro/1/'+
            'mrro_day_inmcm4_esmHistorical_r1i1p1_19800101-19891231.nc')
    """

    part = path.split('/')
    session = Session()

    dataset_id = get_or_insert(session, Dataset,
            activity   = part[0],
            product    = part[1],
            institute  = part[2],
            model      = part[3],
            experiment = part[4],
            frequency  = part[5],
            realm      = part[6],
            MIP        = part[7],
            ensemble   = part[8],
            variable   = part[10],
            )

    version_id = get_or_insert(session, Version,
            dataset_id = dataset_id,
            version    = part[9],
            )

    file_id = get_or_insert(session, File,
            version_id = version_id,
            path       = path,
            )

