# This collects all functions to update database using SQLalchemy
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

from __future__ import print_function

import os
import glob
import pickle
import subprocess
from collections import defaultdict

from sqlalchemy.orm.exc import NoResultFound
from ARCCSSive.CMIP5.Model import Instance, Version, VersionFile, VersionWarning


def search_item(db, klass, **kwargs):
    """
    Search for item in DB, if it can't be found return False
    Otherwise returns only first item
    """
    try:
        item = db.query(klass).filter_by(**kwargs).one()
    except NoResultFound:
        return False
    return item

def insert_unique(db, klass, **kwargs):
    """
    Insert an item into the DB if it can't be found
    NB need to commit before terminating session
    Returns id, True if new item, id False if existing
    """
    item = search_item(db, klass, **kwargs)
    new=False
    if not item:
        item = klass(**kwargs)
        db.add(item)
        new=True
        #print("before",item.id)
        db.commit() 
        #print("after",item.id)
    return item, new

def update_item_old(db, klass, item_id, **kwargs):
    """
    Update an existing item into the DB 
    NB need to commit before terminating session
    """
    item=search_item(db, klass, id=item_id)
    if not item:
       print("Warning cannot update item does not exist yet") 
       return 
    for k,v in kwargs.items():
        item.k = v
    # is this correct only to update?? surely not!
    db.add(item)
    db.commit()
    return item

def add_bulk_items(db, klass, rows):
    """Batched INSERT statements via the ORM "bulk", using dictionaries.
       input: rows is a list of dictionaries """
    db.bulk_insert_mappings(klass,rows) 
    db.commit()
    return

def update_item(db, klass, item_id, **kwargs):
    '''
    '''
    db.query(klass).filter(id == item_id).update(**kwargs)
    db.commit()
    return

def commit_changes(db):
    ''' Commit changes to database '''
    return db.commit()

