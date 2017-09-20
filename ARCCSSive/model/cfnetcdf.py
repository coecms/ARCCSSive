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
from sqlalchemy.types import Text, Integer
from sqlalchemy.sql.expression import case

import os

cf_variable_link = Table('cf_variable_link', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cf_attributes_raw.md_hash'), primary_key=True),
        Column('variable_id', Integer, ForeignKey('cf_variable.id'), primary_key=True),
        )

class File(Base):
    """
    A CF-compliant NetCDF file's attributes
    """
    __tablename__ = 'cf_attributes_raw'

    md_hash     = Column(UUID, ForeignKey('paths.pa_hash'), ForeignKey('metadata.md_hash'), primary_key = True)

    #: str: File title
    title       = association_proxy('extra_rel','title')
    #: str: Generating institution
    institution = association_proxy('extra_rel','institution')
    #: str: Dataset source
    source      = association_proxy('extra_rel','source')
    #: str: Data collection this file belongs to
    collection  = Column(Text)

    path_rel = relationship('base.Path')
    extra_rel = relationship('FileExtra')

    #: str: Path to data file
    path = association_proxy('path_rel', 'path')

    #: list[:class:`Variable`]: Component variables
    variables = relationship(
            "Variable",
            secondary=cf_variable_link,
            back_populates='files',
            viewonly=True)

    metadata_netcdf_rel = relationship(
            "base.Metadata",
            primaryjoin='and_(cfnetcdf.File.md_hash == base.Metadata.md_hash,'
                'base.Metadata.md_type == "netcdf")',
            viewonly=True,
            uselist=False)

    metadata_checksum_rel = relationship(
            "base.Metadata",
            primaryjoin='and_(cfnetcdf.File.md_hash == base.Metadata.md_hash,'
                'base.Metadata.md_type == "checksum")',
            viewonly=True,
            uselist=False)

    #: dict: Full metadata
    attributes = association_proxy('metadata_netcdf_rel', 'md_json')

    __mapper_args__ = {
            'polymorphic_on': case([
                (collection == 'CMIP5', 'CMIP5')
                ], else_='unknown'),
            'polymorphic_identity': 'unknown',
            }

    def open(self):
        """
        Open the file
        """
        return xarray.open_dataset(self.path)

    @property
    def filename(self):
        return os.path.basename(self.path)

    @property
    def md5(self):
        return self.metadata_checksum_rel.md_json['md5']

    @property
    def sha256(self):
        return self.metadata_checksum_rel.md_json['sha256']

    def __str__(self):
        return self.filename

class FileExtra(Base):
    __tablename__ = 'cf_attributes'
    md_hash     = Column(UUID, ForeignKey('cf_attributes_raw.md_hash'), primary_key = True)

    #: str: File title
    title       = Column(Text)
    #: str: Generating institution
    institution = Column(Text)
    #: str: Dataset source
    source      = Column(Text)

class Variable(Base):
    """
    A CF-Compliant variable
    """
    __tablename__ = 'cf_variable'

    id             = Column(Integer, primary_key=True)

    #: Variable standard name
    name           = Column(Text)
    #: Canonical unit
    canonical_unit = Column(Text)
    #: Grib code
    grib           = Column(Text)
    #: AMIP name
    amip           = Column(Text)
    #: Description of the variable
    description    = Column(Text)

    aliases_rel    = relationship('VariableAlias', back_populates='variable')

    #: list[str]: Aliases of this variable
    aliases        = association_proxy('aliases_rel','name', 
            creator=lambda a: VariableAlias(name=a))

    #: list[:class:`File`]: Files containing this variable
    files          = relationship(
            'cfnetcdf.File',
            secondary = cf_variable_link,
            back_populates = 'variables')

class VariableAlias(Base):
    __tablename__ = 'cf_variable_alias'

    id             = Column(Integer, primary_key=True)
    variable_id    = Column(Integer, ForeignKey('cf_variable.id'))

    #: Alias name
    name           = Column(Text)

    #: :class:`Variable`: Variable this is an alias to
    variable       = relationship('Variable', back_populates='aliases_rel')
