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

from Session import engine

Base = declarative_base()

class CMIP5Output(Base):
    """Holds the main DRS entry

    Retrieve a iterable list of outputs from the database using CMIP5.query()
    """
    __tablename__ = 'cmip5_output'

    id         = Column(Integer, primary_key=True)
    activity   = Column(String)
    product    = Column(String)
    institute  = Column(String)
    model      = Column(String)
    experiment = Column(String)
    frequency  = Column(String)
    realm      = Column(String)
    variable   = Column(String)
    MIP        = Column(String)
    ensemble   = Column(String)
    version    = Column(String)

    def dataset():
        """Returns an xray.Dataset() containing the output
        """
        pass

class CMIP5File(Base):
    """Holds a single output file
    """
    __tablename__ = 'cmip5_file'

    id         = Column(Integer, primary_key=True)
    path       = Column(String)
    start_date = Column(String)
    final_date = Column(String)

    output_id  = Column(Integer, ForeignKey('cmip5_output.id'))
    output     = relationship("CMIP5Output", backref=backref('files', order_by=id))

Base.metadata.create_all(engine)
