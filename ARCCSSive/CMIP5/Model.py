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

from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

import os
import pwd
import netCDF4
import datetime

Base = declarative_base()

class Variable(Base):
    """
    A model variable from a specific run
    """
    __tablename__ = 'variable'
    id         = Column(Integer, primary_key = True)

    variable   = Column(String) #: Variable name
    experiment = Column(String) #: CMIP experiment
    mip        = Column(String) #: MIP table specifying output frequency
    model      = Column(String) #: Model that generated the dataset
    ensemble   = Column(String) #: Ensemble member

    versions   = relationship('Version', order_by='Version.version', backref='variable') #: List of :class:`Version` for this model variable

class Version(Base):
    """
    A version of a model run's variable

    .. attribute:: variable

        :class:`Variable` associated with this version
    """
    __tablename__ = 'version'
    id          = Column(Integer, primary_key = True)
    variable_id = Column(Integer, ForeignKey('variable.id'))

    version     = Column(String) #: Version identifier
    path        = Column(String) #: Path to the output directory

    files       = relationship('File', order_by='File.start', backref='version') #: List of :py:class:`File` for this version

class File(Base):
    """
    An individual output file

    .. attribute:: version

        :class:`Version` associated with this file
    """
    __tablename__ = 'file'
    id         = Column(Integer, primary_key = True)
    version_id = Column(Integer, ForeignKey('version.id'))

    # Filesystem metadata
    path       = Column(String) #: Path to the file
    owner      = Column(String) #: File owner
    modified   = Column(Date)   #: Date file was last modified

    # Netcdf metadata
    start      = Column(Date)   #: Start date of file's data
    end        = Column(Date)   #: End date of file's data

    def __init__(self, version_id, path):
        self.version_id = version_id
        self.path       = path

        stat            = os.stat(path)
        self.owner      = pwd.getpwuid(stat.st_uid).pw_name
        self.modified   = datetime.datetime.fromtimestamp(stat.st_mtime)

        # Need to do some mangling to get a proper datetime object
        data            = netCDF4.Dataset(path)
        start = netCDF4.num2date(
                    data.variables['time'][0],
                    data.variables['time'].units,
                    data.variables['time'].calendar
                ).timetuple()
        self.start      = datetime.datetime(*start[0:6])
        end = netCDF4.num2date(
                    data.variables['time'][-1],
                    data.variables['time'].units,
                    data.variables['time'].calendar
                ).timetuple()
        self.end      = datetime.datetime(*end[0:6])
        data.close()

class Latest(Base):
    __tablename__ = 'cmip5'

    id         = Column(String, primary_key = True)
    variable   = Column(String)
    mip        = Column(String)
    model      = Column(String)
    experiment = Column(String)
    ensemble   = Column(String)
    version    = Column(String)
