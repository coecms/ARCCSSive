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
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.types import Text, Boolean, Integer
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()

class Path(Base):
    """
    The path to a file in the metadata db
    """
    __tablename__ = 'paths'

    pa_hash = Column(UUID, primary_key = True)

    #: str: Path on NCI's filesystem
    path = Column('pa_path', Text)

cf_variable_link = Table('cf_variable_link', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cf_attributes.md_hash')),
        Column('variable_id', UUID, ForeignKey('cf_variable.variable_id')),
        )

class CFFile(Base):
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
    collection  = Column(Text)

    path_rel = relationship('Path')

    #: str: Path to data file
    path = association_proxy('path_rel', 'path')

    #: list[:class:`CFVariable`]: Component variables
    variables = relationship(
            "CFVariable",
            secondary=cf_variable_link,
            back_populates='cf_files')

    __mapper_args__ = {
            'polymorphic_on': collection,
            'polymorphic_identity': 'unknown',
            }

class CFVariable(Base):
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

    #: list[:class:`CFFile`]: Files containing this variable
    cf_files = relationship(
            "CFFile", 
            secondary=cf_variable_link, 
            back_populates='variables')

cmip5_attributes_links = Table('cmip5_attributes_links', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash')),
        Column('dataset_id', UUID, ForeignKey('cmip5_dataset.dataset_id')),
        Column('version_id', UUID, ForeignKey('cmip5_version.version_id')))

class CMIP5File(CFFile):
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

    #: :class:`CMIP5Dataset`: The dataset this file is part of
    dataset = relationship(
            'CMIP5Dataset',
            secondary=cmip5_attributes_links,
            back_populates='files')

    #: :class:`CMIP5Version`: This file's dataset version
    version = relationship(
            'CMIP5Version',
            secondary=cmip5_attributes_links,
            back_populates='files')

    #: list[:class:`CMIP5Warning`]: Warnings associated with this file
    warnings = relationship(
            'CMIP5Warning',
            secondary=cmip5_attributes_links,
            secondaryjoin='cmip5_attributes_links.c.version_id == CMIP5Warning.version_id')

    __mapper_args__ = {'polymorphic_identity': 'CMIP5'}

class CMIP5Version(Base):
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

    #: :class:`CMIP5Dataset`: Dataset associated with this version
    dataset         = relationship('CMIP5Dataset', back_populates='versions')

    #: :class:`CMIP5VersionOverride`: Errata information for this version
    override        = relationship('CMIP5VersionOverride', uselist=False)

    #: list[:class:`CMIP5File`]: Files belonging to this dataset version
    files = relationship(
            'CMIP5File',
            secondary=cmip5_attributes_links,
            back_populates='version')

    #: list[:class:`CMIP5Warning`]: Warnings attached to the datset by users
    warnings = relationship(
            'CMIP5Warning',
            back_populates='dataset_version')

class CMIP5VersionOverride(Base):
    """
    Errata for a CMIP5 dataset version, for cases when the published version_id
    is unset or incorrect

    Editing this table will automatically update the corresponding
    :class:`CMIP5Version`.

    v = session.query(CMIP5Version).first()
    v.override = CMIP5VersionOverride(version_number='v20120101')
    session.add(v)
    """
    __tablename__ = 'cmip5_override_version'

    version_id     = Column(UUID, ForeignKey('cmip5_version.version_id'), primary_key = True)

    #: str: New version number
    version_number = Column(Text)

    #: boolean: True if this is the latest version available
    is_latest      = Column(Boolean)

class CMIP5Dataset(Base):
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

    #: list[:class:`CMIP5Version`]: Available versions of this dataset
    versions = relationship(
            'CMIP5Version',
            back_populates='dataset')

    #: list[:class:`CMIP5Files`]: Files belonging to this dataset
    #: (note this contains multiple different versions)
    files = relationship(
            'CMIP5File',
            secondary=cmip5_attributes_links,
            back_populates='dataset')

    #: list[:class:`CFVariables`]: Variables associated with this dataset's files
    variables = relationship(
            'CFVariable',
            secondary='join(cmip5_attributes_links, cf_variable_link, '
                'cmip5_attributes_links.c.md_hash ==  cf_variable_link.c.md_hash)',
            primaryjoin='CMIP5Dataset.dataset_id == cmip5_attributes_links.c.dataset_id',
            secondaryjoin='CFVariable.variable_id == cf_variable_link.c.variable_id')

class CMIP5Warning(Base):
    __tablename__ = 'cmip5_warning'

    id = Column(Integer, primary_key=True)
    version_id = Column(UUID, ForeignKey('cmip5_version.version_id'))

    dataset_version = relationship(
            'CMIP5Version',
            back_populates='warnings')
