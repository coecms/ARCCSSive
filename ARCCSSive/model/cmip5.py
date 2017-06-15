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

cmip5_attributes_links = Table('cmip5_attributes_links', Base.metadata,
        Column('md_hash', UUID, ForeignKey('cmip5_attributes.md_hash'), primary_key=True),
        Column('dataset_id', UUID, ForeignKey('cmip5_dataset.dataset_id')),
        Column('version_id', UUID, ForeignKey('cmip5_version.version_id')),
        Column('variable_id', UUID),
        Column('variable_list', ARRAY(Text)))

cmip5_latest_version = Table('cmip5_latest_version', Base.metadata,
        Column('dataset_id', UUID, ForeignKey('cmip5_dataset.dataset_id')),
        Column('version_id', UUID, ForeignKey('cmip5_version.version_id'), primary_key=True),
        Column('variable_id', UUID, primary_key=True))

class File(CFFile):
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
    __tablename__ = 'cmip5_attributes'

    md_hash               = Column(UUID, ForeignKey('cf_attributes_raw.md_hash'), primary_key = True)
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

    dataset = relationship(
            'Dataset',
            uselist=False,
            secondary=cmip5_attributes_links)

    version = relationship(
            'cmip5.Version',
            uselist=False,
            secondary=cmip5_attributes_links,
            back_populates='files')

    warnings = relationship(
            'Warning',
            secondary=cmip5_attributes_links,
            secondaryjoin='cmip5_attributes_links.c.version_id == Warning.version_id')

    timeseries = relationship(
            'Timeseries',
            uselist=False,
            secondary = cmip5_attributes_links,
            secondaryjoin = 'and_('
                'Timeseries.version_id == cmip5_attributes_links.c.version_id,'
                'Timeseries.variable_id == cmip5_attributes_links.c.variable_id'
                ')',
            back_populates = 'files')

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
            order_by='Warning.added_on',
            back_populates='dataset_version')

    variables = relationship(
            'Timeseries',
            back_populates='version',
            viewonly = True)

    def open(self):
        """
        Open all variables in the dataset
        """
        pass

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
    #: str: Ensemble member
    ensemble_member = Column(Text)
    #: str: MIP Table
    mip_table = Column(Text)

    #: list[:class:`Version`]: Available versions of this dataset, in release order
    versions = relationship(
            'cmip5.Version',
            back_populates='dataset',
            order_by='(cmip5.Version.is_latest, cmip5.Version.version_number)')

    #: list[:class:`Timeseries`]: The most recent versions of the variables in this dataset
    variables = relationship('Timeseries',
            secondary=cmip5_latest_version,
            secondaryjoin='and_(Timeseries.version_id == cmip5_latest_version.c.version_id,'
                'Timeseries.variable_id == cmip5_latest_version.c.variable_id)',
            back_populates='dataset')

    @property
    def latest_version(self):
        """
        The latest :class:`Version` for this dataset
        """
        return self.versions[-1]

    def drstree_path(self):
        """
        Get the drs tree path to variables within this dataset
        """
        base = '/g/data1/ua6/DRSv2/CMIP5'
        return os.path.join(
                base,
                self.model_id,
                self.experiment_id,
                self.frequency,
                self.modeling_realm,
                self.ensemble_member)

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

    dataset_version = relationship(
            'cmip5.Version',
            back_populates='warnings')

    def __str__(self):
        return u'%s (%s): %s'%(self.added_on, self.added_by, self.warning) 

class Timeseries(Base):
    """
    All the files at a given Dataset, Variable and Version
    """
    __tablename__ = 'cmip5_timeseries_link'

    dataset_id = Column(UUID, ForeignKey('cmip5_dataset.dataset_id'))
    version_id = Column(UUID, ForeignKey('cmip5_version.version_id'), primary_key=True)
    variable_id = Column(UUID, primary_key=True)
    variable_list = Column(ARRAY(Text))

    __table_args__ = (
            ForeignKeyConstraint(
                ['version_id', 'variable_id'],
                ['cmip5_attributes_links.version_id', 'cmip5_attributes_links.variable_id'],
                ),
            )

    #: Dataset this timeseries is part of
    dataset = relationship('Dataset', back_populates='variables')
    #: Dataset version of this timeseries
    version = relationship('cmip5.Version', back_populates='variables', viewonly=True)

    #: List of files in this timeseries
    files = relationship('cmip5.File', 
            secondary = cmip5_attributes_links,
#            primaryjoin = 'and_('
#                'Timeseries.version_id == cmip5_attributes_links.c.version_id,'
#                'Timeseries.variable_id == cmip5_attributes_links.c.variable_id'
#                ')',
            back_populates = 'timeseries')
    
    warnings = association_proxy('version', 'warnings')

    def open(self):
        """
        Open all files in the set
        """
        return xarray.concat([x.open() for x in self.files], 'time')
