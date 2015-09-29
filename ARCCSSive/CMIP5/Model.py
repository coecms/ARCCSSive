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
    A variable from a model ensemble run

    Query the catalog using SQLalchemy::

        from ARCCSSive import CMIP5
        from ARCCSSive.CMIP5.Model import Variable

        catalog = CMIP5.connect()
        for var in catalog.query(Variable).filter_by(model='ACCESS1-0'):
            print var.experiment, var.variable

    :attribute: model
    :attribute: experiment
    :attribute: variable
    :attribute: mip
    :attribute: ensemble
    :attribute: version A list of :py:class:`Version` for this variable
    """
    __tablename__ = 'variable'
    id         = Column(Integer, primary_key = True)

    model      = Column(String)
    experiment = Column(String)
    variable   = Column(String)
    mip        = Column(String)
    ensemble   = Column(String)

    versions   = relationship('Version', order_by='Version.version', backref='variable')

class Version(Base):
    """
    A version of a model run's variable

    :attribute: version Version identifier
    :attribute: path Path to the output directory
    :attribute: version The matching :py:class:`Variable`
    :attribute: files A list of :py:class:`File` for this variable version
    """
    __tablename__ = 'version'
    id          = Column(Integer, primary_key = True)
    variable_id = Column(Integer, ForeignKey('variable.id'))

    version     = Column(String)
    path        = Column(String)

    files       = relationship('File', order_by='File.start', backref='version')

class File(Base):
    """
    An individual output file

    :attribute: path Path to the file
    :attribute: owner Owner of the file
    :attribute: modified Date file was last modified

    :attribute: start First date of model data in this file
    :attribute: end Final date of model data in this file
    """
    __tablename__ = 'file'
    id         = Column(Integer, primary_key = True)
    version_id = Column(Integer, ForeignKey('version.id'))

    # Filesystem metadata
    path       = Column(String)
    owner      = Column(String)
    modified   = Column(Date)

    # Netcdf metadata
    start      = Column(Date)
    end        = Column(Date)

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
