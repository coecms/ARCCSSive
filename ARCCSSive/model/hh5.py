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
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.ext.indexable import index_property
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.types import Text, Boolean, BigInteger
from sqlalchemy.orm import relationship

from .base import Base

schema = 'hh5'

class pg_json_property(index_property):
    def __init__(self, attr_name, index, cast_type):
        super(pg_json_property, self).__init__(attr_name, index)
        self.cast_type = cast_type

    def expr(self, model):
        expr = super(pg_json_property, self).expr(model)
        return expr.astext.cast(self.cast_type)

class Path(Base):
    __tablename__ = 'paths'
    __table_args__ = {
            'schema': schema,
            }

    pa_hash = Column(UUID, primary_key = True)
    path = Column('pa_path', Text)
    parents = Column('pa_parents', ARRAY(UUID))

    meta = relationship('model.hh5.Metadata',
            uselist = False,
            viewonly = True)

class Metadata(Base):
    __tablename__ = 'metadata'
    __table_args__ = {
            'schema': schema,
            }

    md_hash = Column(UUID, ForeignKey('hh5.paths.pa_hash'), primary_key = True)
    md_type = Column(Text, primary_key = True)
    md_json = Column(JSONB)

    path = relationship('model.hh5.Path',
            uselist = False,
            viewonly = True)

    __mapper_args__ = {
            'polymorphic_on': md_type,
            }

class PosixFile(Metadata):
    __mapper_args__ = {
            'polymorphic_identity': 'posix',
            }

    user = index_property('md_json', 'user')
    group = index_property('md_json', 'group')
    size = pg_json_property('md_json', 'size', BigInteger)

