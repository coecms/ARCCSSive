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

from ARCCSSive.db import connect, Session
from ARCCSSive.model.hh5 import *
from ARCCSSive.model.roadmap import Disks, loadSession 

from sqlalchemy.sql import func
from sqlalchemy import create_engine

import xml.etree.ElementTree
import os
from datetime import datetime, date
from tabulate import tabulate
import argparse

def roadmap_session():
    """
    Open a connection to roadmap database to read/update the disks table
    """
    f=open('/home/581/pxp581/.roadmap')
    db_user, db_psswrd = f.readlines()[0][:-1].split(" ") 
    f.close()
    engine = create_engine('mysql+pymysql://'+ db_user + ":" + db_psswrd + '@144.6.225.37/roadmapdev') 
    try:
        rm_session = engine.connect()
    except Exception as e:
        print('Cannot connect to roadamap database', e)
    return rm_session


def process_path_admin(path, size, rows, rm_session):
    """
    Compare the disk usage with the allocation
    """
    name = os.path.basename(path)
    size_tb = float(size) / 1024.0**4
    messages = []

    disks = rows.filter(Disks.dirname == name).all()
    if len(disks) > 1:
        messages.append('Multiple allocations for this directory')

    try:
        old_size_tb = disks[0].size
        quota_tb = disks[0].allocation
        expire_date = disks[0].expire
        check_date = disks[0].checked_at
        managers = disks[0].managers
        contact = disks[0].contact
        disks[0].size = size_tb
        disks[0].checked_at = date.today() 
        rm_session.commit()
        

    except IndexError:
        expire_date = date.min
        quota_tb = 0
        old_size__tb = 0
        managers = ''
        contact = ''
        messages.append('No allocation')

    if size_tb > quota_tb:
        messages.append('Over quota')
        messages.append('Contact: '+ contact)

    if expire_date > date.today():
        messages.append('Allocation expired')

    return [name, managers, size_tb, quota_tb, ', '.join(messages)]


def main():
    parser = argparse.ArgumentParser(description="Shows allocations for project hh5 (Climate LIEF)")
    parser.add_argument('--debug', action='store_true',
            help="Show database actions")
    parser.add_argument('--admin', action='store_true',
            help="Get and update allocations from roadmap"),
    parser.add_argument('--requests', default='/g/data1/ua8/pxp581/requests_storage.xml',
            help="XML file containing allocation requests")
    args = parser.parse_args()

    connect(echo = args.debug)
    session = Session()

    # Get the id of the base path
    base = session.query(Path.pa_hash).filter_by(path='/g/data3/hh5/tmp').scalar()

    # Sum up the sizes of each level 5 directory
    sums = (session.query(Path.parents[5].label('parent_hash'),
                func.sum(PosixFile.size).label('size'))
            .select_from(PosixFile)
            .join(PosixFile.path)
            .filter(Path.parents[4] == base)
            .group_by(Path.parents[5])).subquery()

    # Convert the level 5 path hash to a path name
    q = session.query(Path.path, sums.c.size).select_from(sums).join(Path, Path.pa_hash == sums.c.parent_hash)
    
    if args.admin:
        rm_session = loadSession(args.debug)
        rows = rm_session.query(Disks)
    else:
        etree = xml.etree.ElementTree.parse(args.requests).getroot()
 


    def process_path(path, size):
        """
        Compare the disk usage with the allocation
        """
        name = os.path.basename(path)
        size_tb = float(size) / 1024.0**4
        messages = []

        disks = etree.findall("disk[dirname='%s']"%name)
        if len(disks) > 1:
            messages.append('Multiple allocations for this directory')

        try:
            quota_tb = float(disks[0].find('size').text)
            expire_date = datetime.strptime(disks[0].find('expire').text, "%Y-%m-%d")
            managers = disks[0].find('managers').text

        except IndexError:
            expire_date = datetime.min
            quota_tb = 0
            managers = ''
            messages.append('No allocation')

        if size_tb > quota_tb:
            messages.append('Over quota')

        if expire_date > datetime.now():
            messages.append('Allocation expired')

        return [name, managers, size_tb, quota_tb, ', '.join(messages)]

    if args.admin:
        print(tabulate(
            [process_path_admin(path, size, rows, rm_session) for path, size in q],
            headers = ['Project', 'Managers', 'Size (Tb)', 'Quota (Tb)', 'Warnings']))
    else:
        print(tabulate(
            [process_path(path, size) for path, size in q],
            headers = ['Project', 'Managers', 'Size (Tb)', 'Quota (Tb)', 'Warnings']))

if __name__ == '__main__':
    main()
