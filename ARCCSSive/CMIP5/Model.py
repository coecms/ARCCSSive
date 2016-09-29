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

from __future__ import print_function
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Date, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from ARCCSSive.data import *

import os
import glob

Base = declarative_base()

class Instance(Base):
    """
    A model variable from a specific run

    Search through these using :func:`ARCCSSive.CMIP5.Session.outputs()`

    .. attribute:: variable

        Variable name

    .. attribute:: experiment

        CMIP experiment

    .. attribute:: mip

        MIP table specifying output frequency and realm

    .. attribute:: model

        Model that generated the dataset

    .. attribute:: ensemble

        Ensemble member

    .. attribute:: realm

        Realm: ie atmos, ocean

    .. attribute:: versions

        List of :class:`Version` available for this output
    """
    __tablename__ = 'instances'
    id         = Column(Integer, name='instance_id', primary_key = True)

    variable   = Column(String, index=True)
    experiment = Column(String, index=True)
    mip        = Column(String, index=True)
    model      = Column(String, index=True)
    ensemble   = Column(String)
    realm      = Column(String)
    # Missing versions are labelled NA in database and v20110427 in drstree, this is CMOR documentation date
    # order doesn't work if version NA

    versions   = relationship('Version', order_by='Version.version', backref='variable')

    __table_args__ = (
            UniqueConstraint('variable','experiment','mip','model','ensemble'),
            )

    def latest(self):
        """
        Returns latest version/s available on raijin, first check in any version is_latest, then checks date stamp
        """
        if len(self.versions)==1: return self.versions 
        vlatest=[v for v in self.versions if v.is_latest]
        if vlatest==[]: 
            valid=[v for v in self.versions if v.version!="NA"]
            if valid==[]: return self.versions
            valid.sort(key=lambda x: x.version[-8:])
            vlatest.append(valid[-1])
            i=-2
            while i>=-len(valid) and valid[i].version[-8:]==vlatest[0].version[-8:]:
                vlatest.append(valid[i])
                i+=-1
        return vlatest
        
    def filenames(self):
        """
        Returns the file names from the latest version of this variable

        :returns: List of file names
        """
        return self.latest()[0].filenames()

    def drstree_path(self):
        """ 
        Returns the drstree path for this instance latest version 
        """
        #drs_root="/g/data1/ua6/drstree/CMIP5/" # this should be passed as DRSTREE env var
        drs_root="/g/data1/ua6/DRSv2/CMIP5/" # pointing to temporary location for new drstree 
        frequency=mip_dict[self.mip][0]
        return drs_root + "/".join([ self.model, 
                                     self.experiment,
                                     frequency,
                                     self.realm,
                                     self.ensemble,
                                     self.variable]) + "/latest" 

# Add alias to deprecated name
Variable = Instance

class Version(Base):
    """
    A version of a model run's variable

    .. attribute:: version

        Version identifier

    .. attribute:: path

        Path to the output directory

    .. attribute:: variable

        :class:`Variable` associated with this version

    .. attribute:: warnings

        List of :class:`VersionWarning` available for this output

    .. attribute:: files

        List of :class:`VersionFile` available for this output

    .. testsetup::

        >>> cmip5  = getfixture('session')

    >>> version = cmip5.query(Version).first()
    """
    __tablename__ = 'versions'
    id          = Column(Integer, name='version_id', primary_key = True)
    instance_id = Column(Integer, ForeignKey('instances.instance_id'), index=True)

    version     = Column(String)
    path        = Column(String)
    dataset_id  = Column(String)
    is_latest   = Column(Boolean)
    checked_on  = Column(String)
    to_update   = Column(Boolean)

    warnings   = relationship('VersionWarning', order_by='VersionWarning.id', 
                              backref='version', cascade="all, delete-orphan", passive_deletes=True)
    files   = relationship('VersionFile', order_by='VersionFile.id', 
                            backref='version', cascade="all, delete-orphan", passive_deletes=True)


    def glob(self):
        """
        Get the glob string matching the CMIP5 filename

        .. testsetup::

            >>> import six
            >>> cmip5  = getfixture('session')
            >>> version = cmip5.query(Version).first()

        >>> six.print_(version.glob())
        a_6hrLev_c_d_e*.nc
        """
        return '%s_%s_%s_%s_%s*.nc'%(
            self.variable.variable,
            self.variable.mip,
            self.variable.model,
            self.variable.experiment,
            self.variable.ensemble)

    def build_filepaths(self):
        """
        Returns the list of files matching this version

        :returns: List of file names

        .. testsetup::

            >>> cmip5  = getfixture('session')
            >>> version = cmip5.query(Version).first()

        >>> version.build_filepaths()
        []
        """
        g = os.path.join(self.path, self.glob())
        return glob.glob(g)
         
    def filenames(self):
        """
        Returns the list of filenames for this version

        :returns: List of file names

        .. testsetup::

            >>> cmip5  = getfixture('session')
            >>> version = cmip5.query(Version).first()

        >>> version.filenames()
        []
        """
        return [x.filename for x in self.files] 
         
    def tracking_ids(self):
        """
        Returns the list of tracking_ids for files in this version

        :returns: List of tracking_ids

        .. testsetup::

            >>> cmip5  = getfixture('session')
            >>> version = cmip5.query(Version).first()

        >>> version.tracking_ids()
        []
        """
        return [x.tracking_id for x in self.files] 

    def drstree_path(self):
        """ 
        Returns the drstree path for this particular version 
        """
        if self.version!='NA':
            version=self.version
        else:
            version='v20110427'
        return self.variable.drstree_path().replace('latest',version)
        
class VersionWarning(Base):
    """
    Warnings associated with a output version
    """
    __tablename__ = 'warnings'

    id         = Column(Integer, name='warning_id', primary_key = True)
    warning    = Column(String)
    added_by   = Column(String)
    added_on   = Column(Date)
    version_id = Column(Integer, ForeignKey('versions.version_id'), index=True)

    def __str__(self):
        return u'%s (%s): %s'%(self.added_on, self.added_by, self.warning) 


class VersionFile(Base):
    """
    Files associated with a output version
    """
    __tablename__ = 'files'

    id           = Column(Integer, name='file_id', primary_key = True)
    filename     = Column(String)
    tracking_id  = Column(String)
    md5          = Column(String)
    sha256       = Column(String)
    version_id   = Column(Integer, ForeignKey('versions.version_id'), index = True)

    def __str__(self):
        return '%s'%(self.filename)
