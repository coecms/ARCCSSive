#!/usr/bin/env python
# Copyright 2018 ARC Centre of Excellence for Climate Systems Science
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
from ARCCSSive.cli.compare_ESGF import *
import mock

def sample_query_yes_no(answer, value):
    with mock.patch('ARCCSSive.cli.compare_ESGF.input', return_value=answer) as inp:
        assert query_yes_no("") == value

def test_query_yes_no():
    sample_query_yes_no("Y", True)
    sample_query_yes_no("y", True)
    sample_query_yes_no("yes", True)
    sample_query_yes_no("yEs", True)

    sample_query_yes_no("N", False)
    sample_query_yes_no("n", False)
    sample_query_yes_no("no", False)
    sample_query_yes_no("nO", False)
