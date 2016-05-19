#!/usr/bin/env python
# Copyright 2016 ARC Centre of Excellence for Climate Systems Science
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
import socket
import sys
import os
from pprint import pformat
from .pkg_info import __version__

class Info:
    def __init__(self):
        self.host= socket.getfqdn(),
        self.version= __version__,
        self.py_version= sys.version,
        self.databases= {
            'CMIP5': os.environ.get('CMIP5_DB',None),
            }

    def __str__(self):
        return pformat(vars(self))

info = Info()
