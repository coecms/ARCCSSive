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
from Model import CMIP5Output, CMIP5File

def insert(path):
    """ Insert a DRS path into the database

    Example:
        
        from ARCCSSive import CMIP5
        CMIP5.insert('CMIP5/output1/INM/inmcm4/esmHistorical/day/land/day/r1i1p1/mrro/1/'+
            'mrro_day_inmcm4_esmHistorical_r1i1p1_19800101-19891231.nc')
    """

    part = path.split('/')
    session = Session()

    output = session.query(CMIP5Output).filter_by(
            activity   = part[0],
            product    = part[1],
            institute  = part[2],
            model      = part[3],
            experiment = part[4],
            frequency  = part[5],
            realm      = part[6],
            MIP        = part[7],
            ensemble   = part[8],
            version    = part[9],
            variable   = part[10],
            )
    output_id = None

    if output.count() > 0:
        output_id = output[0].id
    else:
        output = CMIP5Output(
            activity   = part[0],
            product    = part[1],
            institute  = part[2],
            model      = part[3],
            experiment = part[4],
            frequency  = part[5],
            realm      = part[6],
            MIP        = part[7],
            ensemble   = part[8],
            version    = part[9],
            variable   = part[10],
                )
        session.add(output)
        session.commit()
        output_id = output.id

    cmipfile = CMIP5File(
            path=path,
            output_id=output_id
            )
    session.add(cmipfile)
    session.commit()
