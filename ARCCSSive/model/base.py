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

from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.types import Text, Boolean, Integer, BigInteger
from ARCCSSive.utils.pg_json_property import pg_json_property

Base = declarative_base()

class Inode(Base):
    """
    An inode on the metadata db
    """
    __tablename__ = 'files'
    fi_hash = Column(UUID, primary_key=True)
    filename = Column('fi_name', Text)

class Path(Base):
    """
    The path to a file in the metadata db
    """
    __tablename__ = 'paths'

    pa_hash = Column(UUID, primary_key = True)

    #: str: Path on NCI's filesystem
    path = Column('pa_path', Text)
    parents = Column('pa_parents', ARRAY(UUID))

    #: Posix metadata
    posix = relationship('base.Posix', back_populates='path', uselist=False)
    #: Checksums
    checksum = relationship('base.Checksum', back_populates='path', uselist=False)
    #: Netcdf metadata
    netcdf = relationship('base.Netcdf', back_populates='path', uselist=False)

class Metadata(Base):
    """
    The main metadata table
    """
    __tablename__  = 'metadata'

    md_hash = Column(UUID, ForeignKey('paths.pa_hash'), primary_key = True)
    md_type = Column(Text, primary_key = True)
    md_json = Column(JSONB)

    __mapper_args__ = {
            'polymorphic_on': md_type
            }

    path = relationship('base.Path')

class Posix(Metadata):
    """
    Posix metadata
    """

    __mapper_args__ = {
            'polymorphic_identity': 'posix',
            }
    size = pg_json_property('md_json','size', BigInteger)
    type = pg_json_property('md_json','type', Text)
    user = pg_json_property('md_json','user', Text)
    group = pg_json_property('md_json','group', Text)

class Checksum(Metadata):
    """
    Checksum metadata
    """

    __mapper_args__ = {
            'polymorphic_identity': 'checksum',
            }
    md5 = pg_json_property('md_json','md5', Text)
    sha256 = pg_json_property('md_json','sha256', Text)

class Netcdf(Metadata):
    """
    Netcdf metadata
    """

    __mapper_args__ = {
            'polymorphic_identity': 'checksum',
            }
