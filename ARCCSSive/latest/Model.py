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

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# CREATE TABLE cmip5
#             (id text, variable text, mip text, model text, experiment text, ensemble text, version text);

class File(Base):
    __tablename__ = 'cmip5'

    path       = Column(String, name = 'id', primary_key = True)
    variable   = Column(String)
    mip        = Column(String)
    model      = Column(String)
    experiment = Column(String)
    ensemble   = Column(String)
    version    = Column(String)

    def open(self):
        return open(self.path,'r')
