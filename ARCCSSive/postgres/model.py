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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.types import Text, Boolean
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()

class Path(Base):
    __tablename__ = 'paths'

    pa_hash = Column(UUID, primary_key = True)
    path = Column('pa_path', Text)

class CFAttributes(Base):
    """
    A CF-compliant NetCDF file's attributes
    """
    __tablename__ = 'cf_attributes'

    md_hash     = Column(UUID, ForeignKey('paths.pa_hash'), primary_key = True)
    title       = Column(Text)
    institution = Column(Text)
    source      = Column(Text)

    path = relationship('Path')
    variables = relationship("CFVariable", back_populates='file_attributes')

class CFVariable(Base):
    """
    A variable in a CF-Compliant NetCDF file
    """
    __tablename__ = 'cf_variable'

    variable_id = Column(Text, primary_key = True)
    md_hash     = Column(UUID, ForeignKey('cf_attributes.md_hash'))
    name        = Column(Text)
    units       = Column(Text)
    long_name   = Column(Text)
    axis        = Column(Text)

    file_attributes = relationship("CFAttributes", back_populates='variables')

cmip5_association = Table('cmip5_attributes_derived', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash')),
        Column('dataset_id', Text, ForeignKey('cmip5_dataset.dataset_id')),
        Column('version_id', Text, ForeignKey('cmip5_version.version_id')))

class CMIP5Attributes(CFAttributes):
    """
    A CMIP5 file's attributes
    """
    __tablename__ = 'cmip5_attributes'

    md_hash               = Column(UUID, ForeignKey('cf_attributes.md_hash'), primary_key = True)
    experiment_id         = Column(Text)
    frequency             = Column(Text)
    institute_id          = Column(Text)
    model_id              = Column(Text)
    modeling_realm        = Column(Text)
    product               = Column(Text)
    table_id              = Column(Text)
    tracking_id           = Column(Text)
    version_number        = Column(Text)
    realization           = Column(Text)
    initialization_method = Column(Text)
    physics_version       = Column(Text)

    dataset = relationship('CMIP5Dataset', secondary=cmip5_association, back_populates='file_attributes')
    version = relationship('CMIP5Version', secondary=cmip5_association, back_populates='file_attributes')

class CMIP5Version(Base):
    __tablename__ = 'cmip5_version'

    version_id     = Column(Text, primary_key = True)
    dataset_id     = Column(Text, ForeignKey('cmip5_dataset.dataset_id'))
    version_number = Column(Text)
    is_latest      = Column(Boolean)
    to_update      = Column(Boolean)

    dataset         = relationship('CMIP5Dataset', back_populates='versions')
    file_attributes = relationship('CMIP5Attributes', secondary=cmip5_association, back_populates='version')
    override        = relationship('CMIP5VersionOverride', uselist=False)

class CMIP5VersionOverride(Base):
    __tablename__ = 'cmip5_override_version'

    version_id     = Column(Text, ForeignKey('cmip5_version.version_id'), primary_key = True)
    version_number = Column(Text)
    is_latest      = Column(Boolean)
    to_update      = Column(Boolean)

class CMIP5Dataset(Base):
    __tablename__ = 'cmip5_dataset'

    dataset_id     = Column(Text, primary_key=True)
    experiment_id  = Column(Text)
    institute_id   = Column(Text)
    model_id       = Column(Text)
    modeling_realm = Column(Text)
    frequency      = Column(Text)

    versions = relationship('CMIP5Version', back_populates='dataset')
    file_attributes = relationship('CMIP5Attributes', secondary=cmip5_association, back_populates='dataset')
