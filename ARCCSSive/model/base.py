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

from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.types import Text, Boolean, Integer

Base = declarative_base()

class Inode(Base):
    """
    An inode on the metadata db
    """
    __tablename__ = 'files'
    fi_hash = Column(UUID, primary_key=True)
    filename = Column('fi_file', Text)

class Path(Base):
    """
    The path to a file in the metadata db
    """
    __tablename__ = 'paths'

    pa_hash = Column(UUID, primary_key = True)

    #: str: Path on NCI's filesystem
    path = Column('pa_path', Text)

class Metadata(Base):
    """
    The main metadata table
    """
    __tablename__  = 'metadata'

    md_hash = Column(UUID, ForeignKey('paths.pa_hash'), primary_key = True)
    md_json = Column(JSONB)

    path_rel = relationship('Path')

    #: str: Path to data file
    path = association_proxy('path_rel', 'path')
