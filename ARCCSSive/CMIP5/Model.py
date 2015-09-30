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

from sqlalchemy import Column, Integer, String, ForeignKey, Date, UniqueConstraint
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

import os
import glob
import pwd
import netCDF4
import datetime

Base = declarative_base()

class Variable(Base):
    """
    A model variable from a specific run

    .. attribute:: variable

        Variable name

    .. attribute:: experiment

        CMIP experiment

    .. attribute:: mip

        MIP table specifying output frequency

    .. attribute:: model

        Model that generated the dataset

    .. attribute:: ensemble

        Ensemble member
    """
    __tablename__ = 'variable'
    id         = Column(Integer, primary_key = True)

    variable   = Column(String) #: Variable name
    experiment = Column(String) #: CMIP experiment
    mip        = Column(String) #: MIP table specifying output frequency
    model      = Column(String) #: Model that generated the dataset
    ensemble   = Column(String) #: Ensemble member

    versions   = relationship('Version', order_by='Version.version', backref='variable') #: List of :class:`Version` for this model variable

    __table_args__ = (
            UniqueConstraint('variable','experiment','mip','model','ensemble'),
            )

class Version(Base):
    """
    A version of a model run's variable

    .. attribute:: version

        Version identifier

    .. attribute:: path

        Path to the output directory

    .. attribute:: files

        List of :py:class:`File` for this version

    .. attribute:: variable

        :class:`Variable` associated with this version
    """
    __tablename__ = 'version'
    id          = Column(Integer, primary_key = True)
    variable_id = Column(Integer, ForeignKey('variable.id'))
    latest_id   = Column(Integer, ForeignKey('cmip5.id'))

    version     = Column(String) #: Version identifier
    path        = Column(String) #: Path to the output directory

    def glob(self):
        file = '%s_%s_%s_%s_%s*.nc'%(
            self.variable.variable,
            self.variable.mip,
            self.variable.model,
            self.variable.experiment,
            self.variable.ensemble)
        return os.path.join(self.path, file)

    def files(self):
        return glob.glob(self.glob())

class Latest(Base):
    __tablename__ = 'cmip5'

    id         = Column(Integer, primary_key = True)
    path       = Column(String)
    variable   = Column(String)
    mip        = Column(String)
    model      = Column(String)
    experiment = Column(String)
    ensemble   = Column(String)
    version    = Column(String)
