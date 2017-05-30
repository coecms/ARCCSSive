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

from .model import *

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker

import os

default_url = os.environ.get('ARCCSSIVE_DB', 'postgres://130.56.244.109:5432/postgres')
default_user = os.environ.get('ARCCSSIVE_USER', os.environ.get('USER',''))

Session = sessionmaker()

def connect(url=default_url, user=default_user, password=None, echo=False):
    """
    Connect to the database
    """
    _url = make_url(url)

    try:
        import private
        _url.username = private.username
        _url.password = private.password
    except ImportError:
        if user is not None:
            _url.username = user
        if password is not None:
            _url.password = password

    engine = create_engine(_url, echo=echo)
    Session.configure(bind=engine)
    return engine
