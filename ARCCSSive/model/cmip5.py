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

from .base import Base
from .cfnetcdf import File as CFFile, Variable

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.types import Text, Boolean, Integer

cmip5_attributes_links = Table('cmip5_attributes_links', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash')),
        Column('dataset_id', UUID, ForeignKey('cmip5_dataset.dataset_id')),
        Column('version_id', UUID, ForeignKey('cmip5_version.version_id')))

class File(CFFile):
    """
    A CMIP5 output file's attributes
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

    #: :class:`Dataset`: The dataset this file is part of
    dataset = relationship(
            'Dataset',
            secondary=cmip5_attributes_links,
            back_populates='files')

    #: :class:`Version`: This file's dataset version
    version = relationship(
            'Version',
            secondary=cmip5_attributes_links,
            back_populates='files')

    #: list[:class:`Warning`]: Warnings associated with this file
    warnings = relationship(
            'Warning',
            secondary=cmip5_attributes_links,
            secondaryjoin='cmip5_attributes_links.c.version_id == Warning.version_id')

    __mapper_args__ = {'polymorphic_identity': 'CMIP5'}

class Version(Base):
    """
    A version of a ESGF dataset

    Over time files within a dataset get updated, due to bug fixes and
    processing improvements. This results in multiple versions of files getting
    published to ESGF
    """
    __tablename__ = 'cmip5_version'

    version_id     = Column(UUID, primary_key = True)
    dataset_id     = Column(Text, ForeignKey('cmip5_dataset.dataset_id'))

    #: str: Version number
    version_number = Column(Text)

    #: boolean: True if this is the latest version available
    is_latest      = Column(Boolean)

    #: :class:`Dataset`: Dataset associated with this version
    dataset         = relationship('Dataset', back_populates='versions')

    #: :class:`VersionOverride`: Errata information for this version
    override        = relationship('VersionOverride', uselist=False)

    #: list[:class:`File`]: Files belonging to this dataset version
    files = relationship(
            'cmip5.File',
            secondary=cmip5_attributes_links,
            back_populates='version')

    #: list[:class:`Warning`]: Warnings attached to the datset by users
    warnings = relationship(
            'Warning',
            back_populates='dataset_version')

class VersionOverride(Base):
    """
    Errata for a CMIP5 dataset version, for cases when the published version_id
    is unset or incorrect

    Editing this table will automatically update the corresponding
    :class:`Version`.

    v = session.query(Version).first()
    v.override = VersionOverride(version_number='v20120101')
    session.add(v)
    """
    __tablename__ = 'cmip5_override_version'

    version_id     = Column(UUID, ForeignKey('cmip5_version.version_id'), primary_key = True)

    #: str: New version number
    version_number = Column(Text)

    #: boolean: True if this is the latest version available
    is_latest      = Column(Boolean)

class Dataset(Base):
    """
    A CMIP5 Dataset, as you'd find listed on ESGF
    """
    __tablename__ = 'cmip5_dataset'

    dataset_id     = Column(UUID, primary_key=True)
    experiment_id  = Column(Text)

    #: str: ID of the institute that ran the experiment
    institute_id   = Column(Text)
    #: str: ID of the model used
    model_id       = Column(Text)
    #: str: Model component - atmos, land, ocean, etc.
    modeling_realm = Column(Text)
    #: str: Data output frequency
    frequency      = Column(Text)

    #: list[:class:`Version`]: Available versions of this dataset, in release order
    versions = relationship(
            'Version',
            back_populates='dataset',
            order_by='(Version.is_latest, Version.version_number)')

    #: list[:class:`Files`]: Files belonging to this dataset
    #: (note this contains multiple different versions)
    files = relationship(
            'cmip5.File',
            secondary=cmip5_attributes_links,
            back_populates='dataset')

    #: list[:class:`Variable`]: Variables associated with this dataset's files
    variables = relationship(
            'Variable',
            secondary='join(cmip5_attributes_links, cf_variable_link, '
                'cmip5_attributes_links.c.md_hash ==  cf_variable_link.c.md_hash)',
            primaryjoin='Dataset.dataset_id == cmip5_attributes_links.c.dataset_id',
            secondaryjoin='Variable.variable_id == cf_variable_link.c.variable_id')

    @property
    def latest_version(self):
        """
        The latest :class:`Version` for this dataset
        """
        return self.versions[-1]

class Warning(Base):
    __tablename__ = 'cmip5_warning'

    id = Column(Integer, primary_key=True)
    version_id = Column(UUID, ForeignKey('cmip5_version.version_id'))

    dataset_version = relationship(
            'Version',
            back_populates='warnings')
