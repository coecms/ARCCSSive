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

Base = declarative_base()

class Variable(Base):
    """
    A model variable from a specific run

    Search through these using :func:`ARCCSSive.CMIP5.Session.outputs()`

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

    .. attribute:: versions

        List of :class:`Version` available for this output
    """
    __tablename__ = 'variable'
    id         = Column(Integer, primary_key = True)

    variable   = Column(String)
    experiment = Column(String)
    mip        = Column(String)
    model      = Column(String)
    ensemble   = Column(String)

    versions   = relationship('Version', order_by='Version.version', backref='variable')

    __table_args__ = (
            UniqueConstraint('variable','experiment','mip','model','ensemble'),
            )

    def filenames(self):
        """
        Returns the file names from the latest version of this variable

        :returns: List of file names
        """
        return self.versions[-1].files()

class Version(Base):
    """
    A version of a model run's variable

    .. attribute:: version

        Version identifier

    .. attribute:: path

        Path to the output directory

    .. attribute:: variable

        :class:`Variable` associated with this version
    """
    __tablename__ = 'version'
    id          = Column(Integer, primary_key = True)
    variable_id = Column(Integer, ForeignKey('variable.id'))
    latest_id   = Column(Integer, ForeignKey('cmip5.id'))

    version     = Column(String)
    path        = Column(String)

    def glob(self):
        """ Get the glob string matching the CMIP5 filename
        """
        return '%s_%s_%s_%s_%s*.nc'%(
            self.variable.variable,
            self.variable.mip,
            self.variable.model,
            self.variable.experiment,
            self.variable.ensemble)

    def filenames(self):
        """
        Returns the list of files matching this version

        :returns: List of file names
        """
        g = os.path.join(self.path, self.glob())
        return glob.glob(g)

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
