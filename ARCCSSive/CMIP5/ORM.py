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

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ModelOutput(Base):
    """Holds the main DRS entry
    """
    __tablename__ = 'model_output'

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
