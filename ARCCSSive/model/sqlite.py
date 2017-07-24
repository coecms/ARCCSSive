#!/usr/bin/env python
"""
Copyright 2017 

author:  <>

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
from __future__ import print_function

from .base import Base
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy import BigInteger, Text, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID

sqlite_paths_link = Table('sqlite_paths_link', Base.metadata,
        Column('hash', UUID, ForeignKey('paths.pa_hash'), ForeignKey('metadata.md_hash')),
        Column('file_id', BigInteger, ForeignKey('sqlite_files.file_id'), primary_key=True),
        )

class Instance(Base):
    __tablename__ = 'sqlite_instances'
    instance_id = Column(BigInteger, primary_key=True)
    variable = Column(Text)
    mip = Column(Text)
    model = Column(Text)
    experiment = Column(Text)
    ensemble = Column(Text)
    realm = Column(Text)

    versions = relationship('sqlite.Version', back_populates='instance')

class Version(Base):
    __tablename__ = 'sqlite_versions'
    version_id = Column(BigInteger, primary_key=True)
    path = Column(Text)
    version = Column(Text)
    dataset_id = Column(Text)
    is_latest = Column(Boolean)
    checked_on = Column(Text)
    to_update = Column(Boolean)
    instance_id = Column(BigInteger, ForeignKey('sqlite_instances.instance_id'))

    instance = relationship('sqlite.Instance', back_populates='versions')
    files = relationship('sqlite.File', back_populates='version')

class File(Base):
    __tablename__ = 'sqlite_files'
    file_id = Column(BigInteger, primary_key=True)
    filename = Column(Text)
    tracking_id = Column(Text)
    md5 = Column(Text)
    sha256 = Column(Text)
    version_id = Column(BigInteger, ForeignKey('sqlite_versions.version_id'))

    path = relationship('base.Path', secondary=sqlite_paths_link)
    version = relationship('sqlite.Version', back_populates='files')

class Warning(Base):
    __tablename__ = 'sqlite_warnings'
    warning_id = Column(BigInteger, primary_key=True)
    warning = Column(Text)
    added_by = Column(Text)
    added_on = Column(Text)
    version_id = Column(BigInteger, ForeignKey('sqlite_versions.version_id'))

