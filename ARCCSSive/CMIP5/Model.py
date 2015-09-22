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

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

import xray

Base = declarative_base()

class Dataset(Base):
    """Holds the main DRS entry

    Retrieve a iterable list of outputs from the database using CMIP5.query()
    """
    __tablename__ = 'cmip5_dataset'

    id         = Column(Integer, primary_key=True)
    activity   = Column(String)
    product    = Column(String)
    institute  = Column(String)
    model      = Column(String)
    experiment = Column(String)
    frequency  = Column(String)
    realm      = Column(String)
    variable   = Column(String)
    MIP        = Column(String)
    ensemble   = Column(String)
    version    = relationship("Version", order_by="Version.version", backref="dataset")

    def filenames(self):
        return [x.path for x in self.version[-1].files]

    def dataset(self):
        """Returns an xray.Dataset() containing the most recent version of this dataset

        To retrieve a specific version use the 'version' attribute, e.g::

            version = dataset.version[1].version
            data    = dataset.version[1].dataset()
        """
        return self.version[-1].dataset()

class Version(Base):
    """Used to select different versions of the dataset
    """
    __tablename__ = 'cmip5_version'

    id      = Column(Integer, primary_key=True)
    version = Column(String)
    files   = relationship("File", order_by="File.id", backref="version")

    dataset_id  = Column(Integer, ForeignKey('cmip5_dataset.id'))

    def dataset(self):
        """Returns a xray.Dataset combining all files in this dataset version
        """
        return xray.concat([f.dataset() for f in self.files],'time')

class File(Base):
    """Holds a single output file
    """
    __tablename__ = 'cmip5_file'

    id         = Column(Integer, primary_key=True)
    path       = Column(String)
    start_date = Column(String)
    final_date = Column(String)

    version_id  = Column(Integer, ForeignKey('cmip5_version.id'))

    def dataset(self):
        """Returns a xray.Dataset containing this file's data
        """
        return xray.open_dataset(self.path)

