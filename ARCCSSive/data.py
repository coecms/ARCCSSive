#!/usr/bin/env python
"""
Copyright 2016 ARC Centre of Excellence for Climate Systems Science

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

import pickle

# load mip and frequency dictionaries
def _read_pickle():
    # Read the packaged data
    from pkg_resources import resource_stream
    pickle_stream = resource_stream(__name__, 'data/cmip_dict_pickle')

    mip_dict = pickle.load(pickle_stream)
    frq_dict = pickle.load(pickle_stream)
    model_names_dict = pickle.load(pickle_stream)
    return mip_dict, frq_dict, model_names_dict

mip_dict, frq_dict, model_names_dict = _read_pickle()
