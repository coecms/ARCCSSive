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

from __future__ import print_function, unicode_literals
from sqlalchemy import Column, Integer, Text, Boolean, ForeignKey, Date, UniqueConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import select, func, join
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.session import object_session
from ARCCSSive.data import *

from ARCCSSive.model.base import Base
import ARCCSSive.model.cmip5 as model2

import os
import glob

# Aliases to old names
VersionWarning = model2.Warning
VersionFile = model2.File

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
    __tablename__ = 'old_cmip5_instance'

    dataset_id = Column(UUID, ForeignKey('cmip5_dataset.dataset_id'), primary_key=True)
    variable   = Column(Text, primary_key=True)
    variable_id = Column(UUID)
    experiment = Column(Text)
    mip        = Column(Text)
    model      = Column(Text)
    ensemble   = Column(Text)
    realm      = Column(Text)

    __table_args__ = (
            ForeignKeyConstraint(
                ['dataset_id', 'variable_id'],
                ['cmip5_latest_version.dataset_id', 'cmip5_latest_version.variable_id'],
                ),
            )

    versions = relationship('CMIP5.Model.Version',
            order_by='(CMIP5.Model.Version.is_latest, CMIP5.Model.Version.version)',
            primaryjoin='and_(Instance.dataset_id == CMIP5.Model.Version.dataset_id,'
                'Instance.variable == CMIP5.Model.Version.variable_name)',
            viewonly = True
            )

    latest_version = relationship('CMIP5.Model.Version',
            secondary = model2.cmip5_latest_version,
            uselist = False,
            viewonly = True)

#     # Missing versions are labelled NA in database and v20110427 in drstree, this is CMOR documentation date
#     # order doesn't work if version NA
# 
#     versions   = relationship('Version', order_by='Version.version', backref='variable')
# 
#     __table_args__ = (
#             UniqueConstraint('variable','experiment','mip','model','ensemble'),
#             )
# 
#    def latest(self):
#        """
#        Returns latest version/s available on raijin, first check in any version is_latest, then checks date stamp
#        """
#        if len(self.versions)==1: return self.versions 
#        vlatest=[v for v in self.versions if v.is_latest]
#        if vlatest==[]: 
#            valid=[v for v in self.versions if v.version!="NA"]
#            if valid==[]: return self.versions
#            valid.sort(key=lambda x: x.version[-8:])
#            vlatest.append(valid[-1])
#            i=-2
#            while i>=-len(valid) and valid[i].version[-8:]==vlatest[0].version[-8:]:
#                vlatest.append(valid[i])
#                i+=-1
#        return vlatest

    def latest(self):
        return [self.latest_version]
        
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

    >>> instance = cmip5.query(Instance).filter_by(dataset_id = 'c6d75f4c-793b-5bcc-28ab-1af81e4b679d', variable='tas').one()
    >>> version = instance.latest()
    >>> version = instance.versions[-1]
    """
    __tablename__ = 'old_cmip5_version'

    dataset_id = Column(UUID, ForeignKey('cmip5_dataset.dataset_id'))
    version_id = Column(UUID, ForeignKey('cmip5_version.version_id'), primary_key=True)
    variable_id = Column(UUID)
    variable_name = Column('variable', Text, primary_key=True)

#    id          = Column(Integer, name='version_id', primary_key = True)
#    instance_id = Column(Integer, ForeignKey('instances.instance_id'), index=True)

    version     = Column(Text)
#     path        = Column(Text)
#     dataset_id  = Column(Text)
    is_latest   = Column(Boolean)
#     checked_on  = Column(Text)
#     to_update   = Column(Boolean)
# 
#     warnings   = relationship('VersionWarning', order_by='VersionWarning.id', 
#                               backref='version', cascade="all, delete-orphan", passive_deletes=True)
#     files   = relationship('VersionFile', order_by='VersionFile.id', 
#                             backref='version', cascade="all, delete-orphan", passive_deletes=True)

    __table_args__ = (
            ForeignKeyConstraint(
                ['variable', 'dataset_id'],
                ['old_cmip5_instance.variable', 'old_cmip5_instance.dataset_id'],
                ),
            ForeignKeyConstraint(
                ['version_id', 'variable_id'],
                ['cmip5_latest_version.version_id', 'cmip5_latest_version.variable_id'],
                ),
            )

    timeseries = relationship('cmip5.Timeseries',
            primaryjoin='and_('
                'Model.Version.dataset_id == foreign(cmip5.Timeseries.dataset_id),'
                'Model.Version.version_id == foreign(cmip5.Timeseries.version_id),'
                'cmip5.Timeseries.variable_list.any(Model.Version.variable_name)'
                ')',
            uselist=False,
            viewonly=True)

    new_version = relationship('cmip5.Version')
    warnings = association_proxy('new_version', 'warnings')
    files = association_proxy('timeseries', 'files')

    variable = relationship('Instance', viewonly=True)

    def glob(self):
        """
        Get the glob string matching the CMIP5 filename

        .. testsetup::

            >>> import six
            >>> cmip5  = getfixture('session')
            >>> version = cmip5.query(Version).filter_by(version_id = 'ed04fb7a-79e2-5b2f-2569-42abffd322db', variable_name='tas').one()

        >>> six.print_(version.glob())
        tas_day_ACCESS1.3_rcp45_r1i1p1*.nc
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
            >>> version = cmip5.query(Version).filter_by(version_id = 'ed04fb7a-79e2-5b2f-2569-42abffd322db', variable_name='tas').one()

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
            >>> version = cmip5.query(Version).filter_by(version_id = 'ed04fb7a-79e2-5b2f-2569-42abffd322db', variable_name='tas').one()

        >>> sorted(version.filenames())
        ['tas_day_ACCESS1-3_rcp45_r1i1p1_20060101-20301231.nc', 'tas_day_ACCESS1-3_rcp45_r1i1p1_20310101-20551231.nc', 'tas_day_ACCESS1-3_rcp45_r1i1p1_20560101-20801231.nc', 'tas_day_ACCESS1-3_rcp45_r1i1p1_20810101-21001231.nc']
        """
        return [x.filename for x in self.files] 
         
    def tracking_ids(self):
        """
        Returns the list of tracking_ids for files in this version

        :returns: List of tracking_ids

        .. testsetup::

            >>> cmip5  = getfixture('session')
            >>> version = cmip5.query(Version).filter_by(version_id = 'ed04fb7a-79e2-5b2f-2569-42abffd322db', variable_name='tas').one()

        >>> sorted(version.tracking_ids())
        ['54779e2d-41fb-4671-bbdf-2170385afa3b', '800713b7-c303-4618-aef9-f72548d5ada6', 'd2813685-9c7c-4527-8186-44a8f19d31dd', 'f810f58d-329e-4934-bb1c-28c5c314e073']
        """
        return [x.tracking_id for x in self.files] 

    def drstree_path(self):
        """ 
        Returns the drstree path for this particular version 
        """
        if self.version is not None:
            version=self.version
        else:
            version='v20110427'
        return self.variable.drstree_path().replace('latest',version)
        
#class VersionWarning(Base):
#    """
#    Warnings associated with a output version
#    """
#    __tablename__ = 'warnings'
#
#    id         = Column(Integer, name='warning_id', primary_key = True)
#    warning    = Column(Text)
#    added_by   = Column(Text)
#    added_on   = Column(Text)
#    version_id = Column(Integer, ForeignKey('versions.version_id'), index=True)
#
#    def __str__(self):
#        return u'%s (%s): %s'%(self.added_on, self.added_by, self.warning) 
#
#
#class VersionFile(Base):
#    """
#    Files associated with a output version
#    """
#    __tablename__ = 'files'
#
#    id           = Column(Integer, name='file_id', primary_key = True)
#    filename     = Column(Text)
#    tracking_id  = Column(Text)
#    md5          = Column(Text)
#    sha256       = Column(Text)
#    version_id   = Column(Integer, ForeignKey('versions.version_id'), index = True)
#
#    def __str__(self):
#        return '%s'%(self.filename)
