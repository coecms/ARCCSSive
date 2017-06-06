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
import requests
from xml.etree import ElementTree

from ARCCSSive.model.cfnetcdf import Variable
from ARCCSSive.db import Session, connect

def load_cf_table(session):
    table_url = 'http://cfconventions.org/Data/cf-standard-names/44/src/cf-standard-name-table.xml'
    r = requests.get(table_url)
    tree = ElementTree.fromstring(r.content)

    variables = []

    for entry in tree.iter('entry'):
        v = Variable(
            name = entry.get('id'),
            canonical_unit = entry.find('canonical_units').text,
            grib = entry.find('grib').text,
            amip = entry.find('amip').text,
            description = entry.find('description').text,
            )
        v.aliases.append(entry.get('id'))
        variables.append(v)

    session.add_all(variables)
    session.commit()

    for alias in tree.iter('alias'):
        v = session.query(Variable).filter_by(name=alias.find('entry_id').text).scalar()
        v.aliases.append(alias.get('id'))

    session.commit()

def main():
    connect(echo=True)
    session = Session()
    load_cf_table(session)

if __name__ == '__main__':
    main()
