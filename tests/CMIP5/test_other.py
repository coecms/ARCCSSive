#!/usr/bin/env python
"""
Copyright 2016 ARC Centre of Excellence for Climate Systems Science

author: Paola Petrelli <paola.petrelli@utas.edu.au>

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

from ARCCSSive.CMIP5.other_functions import *

def test_frequency():
    assert frequency('Amon') == ['mon']

def test_constraints():
    data = {'variable': ['a','b'],
            'model': ['m1','m2','m3'],
            'experiment': ['e1','e2'],
            'field4': ['v1']}
    results = list(combine_constraints(**data))
    assert len(results)==12
    for r in results:
       assert type(r) is dict
       assert len(r)==len(data.keys())
    assert {'variable': 'b', 'model': 'm2', 'experiment': 'e1', 'field4': 'v1'} in results
    assert {'variable': 'b', 'model': 'm2', 'field4': 'v1'} not in results

def test_compare_tracking_ids():
    local=['34rth678','de45t','abc123']
    # remote ids same as local, different, empty
    remote=[['34rth678','de45t','abc123'],['34rth678','de45t'],[]]
    output=[set([]),set(['abc123']),set(local)]
    for i in range(3):
       assert compare_tracking_ids(remote[i],local)==output[i]
