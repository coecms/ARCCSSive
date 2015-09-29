#!/usr/bin/env python
"""
file:   bin/import-version.py
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
from ARCCSSive.CMIP5.Model import Base, Version, Latest, Variable
from sqlalchemy import create_engine
from sqlalchemy.sql import select, func

variable = Variable.__table__
version  = Version.__table__
latest   = Latest.__table__

engine = create_engine('sqlite:///latest.db')
Base.metadata.create_all(engine)
conn = engine.connect()

# Insert into version any missing items from latest
latest_ids = select([version.c.latest_id])
missing    = select([latest.c.id, latest.c.path, latest.c.version]).where(latest.c.id.notin_(latest_ids))
insert     = version.insert().from_select(['latest_id','path','version'], missing)

print insert
conn.execute(insert)

# Insert into variable any missing items from latest
unique = select([func.min(latest.c.id)]).group_by(
    latest.c.variable,   
    latest.c.experiment, 
    latest.c.mip,        
    latest.c.model,      
    latest.c.ensemble,   
    )
missing = select([
    latest.c.variable,   
    latest.c.experiment, 
    latest.c.mip,        
    latest.c.model,      
    latest.c.ensemble,   
    ]).where(latest.c.id.notin_(unique))
insert     = variable.insert().from_select(['variable','experiment','mip','model','ensemble'], missing)

print insert
conn.execute(insert)
