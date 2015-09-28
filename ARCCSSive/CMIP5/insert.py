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

def get_or_insert(session, klass, **kwargs):
    """Return the id of the entry matching **kwargs

    Creates a new entry with kwargs if one doesn't exist
    Raises an exception if more than one match
    """
    search = session.query(klass).filter_by(**kwargs)

    if search.count() > 1:
        raise RuntimeError("Too many matches")
    if search.count() == 1:
        return search.first().id
    else:
        entry = klass(**kwargs)
        session.add(entry)
        session.commit()
        return entry.id
