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
from .cfnetcdf import File as CFFile, Variable, cf_variable_link

from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Table, join, ForeignKeyConstraint
from sqlalchemy.orm import relationship, column_property
from sqlalchemy.types import Text, Boolean, Integer, Date

cmip5_file_dataset_link = Table('cmip5_file_dataset_link', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash'), ForeignKey('paths.pa_hash'),
            primary_key = True),
        Column('dataset_id', UUID, ForeignKey('cmip5_dataset.dataset_id')))

cmip5_file_version_link = Table('cmip5_file_version_link', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash'), ForeignKey('paths.pa_hash'),
            primary_key = True),
        Column('version_id', UUID, ForeignKey('cmip5_version.version_id'), primary_key = True))

cmip5_latest_version_link = Table('cmip5_file_version_link', Base.metadata,
        Column('dataset_id', UUID, ForeignKey('cmip5_dataset.dataset_id'), primary_key = True),
        Column('version_id', UUID, ForeignKey('cmip5_version.version_id')))

cmip5_attributes = Table('cmip5_attributes', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cf_attributes_raw.md_hash'), primary_key = True),
        Column('experiment_id', Text),
        Column('frequency', Text),
        Column('institute_id', Text),
        Column('model_id', Text),
        Column('modeling_realm', Text),
        Column('product', Text),
        Column('table_id', Text),
        Column('tracking_id', Text),
        Column('version_number', Text),
        Column('realization', Text),
        Column('initialization_method', Text),
        Column('physics_version', Text),
        )

cmip5_external_attributes = Table('cmip5_external_attributes', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash')),
        Column('variable_id', Integer, ForeignKey('cf_variable.variable_id')),
        )

attribute_join = join(cmip5_attributes, cmip5_external_attributes)

class File(Base):
    """
    A CMIP5 output file's attributes

    Relationships:

    attribute:: dataset
       :class:`Dataset`: The dataset this file is part of

    attribute:: version
       :class:`Version`: This file's dataset version

    attribute:: warnings
       [:class:`Warning`]: Warnings associated with this file

    attribute:: timeseries
       :class:`Timeseries` holding all files in the dataset with the same variables

    Attributes:

    attribute:: experiment_id
    attribute:: frequency
    attribute:: institute_id
    attribute:: model_id
    attribute:: modeling_realm
    attribute:: product
    attribute:: table_id
    attribute:: tracking_id
    attribute:: version_number
    attribute:: realization
    attribute:: initialization_method
    attribute:: physics_version
    """
    __tablename__ = attribute_join

    dataset = relationship(
            'Dataset',
            uselist=False,
            secondary=cmip5_file_dataset_link)

    versions = relationship(
            'cmip5.Version',
            secondary=cmip5_file_version_link,
            back_populates='files')

    variable = relationship(
            'cfnetcdf.Variable'
            )

    warnings = relationship(
            'cmip5.Warning',
            secondary=cmip5_attributes_links,
            secondaryjoin='cmip5_attributes_links.c.version_id == cmip5.Warning.version_id')

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
    #: str: Ensemble member
    ensemble_member = Column(Text)
    #: str: MIP Table
    mip_table = Column(Text)
    #: str: CMIP Product
    product = Column(Text)

    #: list[:class:`Version`]: Available versions of this dataset, in release order
    versions = relationship(
            'cmip5.Version',
            back_populates='dataset',
            order_by='(cmip5.Version.is_latest, cmip5.Version.version_number)')

    latest_version = relationship(
            'cmip5.Version',
            secondary = cmip5_latest_version_link,
            use_list = False,
            view_only = True)

    def __str__(self):
        return 'cmip5.%(product)s.%(institute)s.%(model)s.%(experiment)s.%(time_frequency)s.%(realm)s.%(cmor_table)s.%(ensemble)s'%{
                product: self.product,
                institute: self.institute_id,
                model: self.model_id,
                experiment: self.experiment_id,
                time_frequency: self.frequency,
                realm: self.modelling_realm,
                cmor_table: self.mip_table,
                ensemble: self.ensemble_member}

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
    version = Column(Text)

    #: boolean: True if this is the latest version available
    is_latest      = Column(Boolean)

    #: :class:`Dataset`: Dataset associated with this version
    dataset         = relationship('Dataset', back_populates='versions')

    #: list[:class:`File`]: Files belonging to this dataset version
    files = relationship(
            'cmip5.File',
            secondary=cmip5_file_version_link,
            back_populates='version')

    #: list[:class:`Warning`]: Warnings attached to the datset by users
    warnings = relationship(
            'cmip5.Warning',
            order_by='cmip5.Warning.added_on',
            back_populates='dataset_version')

    def __str__(self):
        return '%(dataset)s.%(version)s'%{
                dataset: self.dataset,
                version: self.version,
                }

class Warning(Base):
    __tablename__ = 'cmip5_warning'

    id = Column(Integer, primary_key=True)
    version_id = Column(UUID, ForeignKey('cmip5_version.version_id'))

    #: str: Warning text
    warning = Column(Text)
    #: str: Who added thge warning
    added_by = Column(Text)
    #: str: Date the warning was added
    added_on = Column(Date)

    #: :class:`Version`: Dataset version this warning is attached to
    dataset_version = relationship(
            'cmip5.Version',
            back_populates='warnings')

    def __str__(self):
        return u'%s (%s): %s'%(self.added_on, self.added_by, self.warning) 
