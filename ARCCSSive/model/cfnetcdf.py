#!/usr/bin/env python
# Copyright 2017 ARC Centre of Excellence for Climate Systems Science
# author: Scott Wales <scott.wales@unimelb.edu.au>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

from .base import Base, Path

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.types import Text

cf_variable_link = Table('cf_variable_link', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cf_attributes.md_hash')),
        Column('variable_id', UUID, ForeignKey('cf_variable.variable_id')),
        )

class File(Base):
    """
    A CF-compliant NetCDF file's attributes
    """
    __tablename__ = 'cf_attributes'

    md_hash     = Column(UUID, ForeignKey('paths.pa_hash'), primary_key = True)

    #: str: File title
    title       = Column(Text)
    #: str: Generating institution
    institution = Column(Text)
    #: str: Dataset source
    source      = Column(Text)
    #: str: Data collection this file belongs to
    collection  = Column(Text)

    path_rel = relationship('Path')

    #: str: Path to data file
    path = association_proxy('path_rel', 'path')

    #: list[:class:`Variable`]: Component variables
    variables = relationship(
            "Variable",
            secondary=cf_variable_link,
            back_populates='cf_files')

    __mapper_args__ = {
            'polymorphic_on': collection,
            'polymorphic_identity': 'unknown',
            }

class Variable(Base):
    """
    A variable in a CF-Compliant NetCDF file
    """
    __tablename__ = 'cf_variable'

    variable_id = Column(UUID, primary_key = True)

    #: str: Variable name
    name        = Column(Text)
    #: str: Variable units
    units       = Column(Text)
    #: str: Long name
    long_name   = Column(Text)

    #: list[:class:`File`]: Files containing this variable
    cf_files = relationship(
            "cfnetcdf.File", 
            secondary=cf_variable_link, 
            back_populates='variables')
