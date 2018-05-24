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
#from ARCCSSive.model.roadmap import Disks, loadSession 

from sqlalchemy.sql import func

import xml.etree.ElementTree
from os.path import expanduser, basename

from datetime import datetime, date
from tabulate import tabulate
import argparse
import requests

#def roadmap_session():
    #"""
    #Open a connection to roadmap database to read/update the disks table
    #"""
#    f=open('/home/581/pxp581/.roadmap')
#    db_user, db_psswrd = f.readlines()[0][:-1].split(" ") 
#    f.close()
#    engine = create_engine('mysql+pymysql://'+ db_user + ":" + db_psswrd + '@144.6.225.37/roadmapdev') 
#    try:
#        rm_session = engine.connect()
#    except Exception as e:
#        print('Cannot connect to roadamap database', e)
#    return rm_session

def parse_args():
    """
    Set and return input arguments
    """
    parser = argparse.ArgumentParser(description="Shows allocations for project hh5 (Climate LIEF)")
    parser.add_argument('--debug', action='store_true',
        help="Show database actions")
    parser.add_argument('--admin', action='store_true',
        help="Get and update allocations from roadmap"),
    parser.add_argument('--requests', default='/g/data1/ua8/pxp581/requests_storage.xml',
        help="XML file containing allocation requests")
    return parser.parse_args()

def get_xml():
    """ 
    Get requests.xml file via api using requests
    """
    global token
    # set api url
    api_url = 'http://144.6.225.151:3000//api/v0/disks.xml'
    # read authoprization token
    # set headers
    headers = { "Content-type": "application/xml",  "Authorization": "Token token="+token}
    # get url
    r = requests.get(api_url, headers=headers)
    r.encoding='utf-8'
    return r.content 

def post_json(json_url, data):
    """
    Post dir size and checked_at date for a disk using json and the api
    """
    global token
    headers = { "Content-type": "application/json",  "Authorization": "Token token="+token}
    r = requests.post(json_url, json=data, headers=headers)
    return r

def main():
    global token
    args = parse_args()
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
       # read user api token 
        f = open(expanduser('~')+'/.roadmapapi', 'r')
        token = f.readline().strip()
        f.close()
       # get allocations list as xml file from api and update file
        response = get_xml()
        args.requests = '/g/data/ua8/pxp581/roadmap_requests.xml'
        f = open(args.requests,'w')
        f.write(response)
        f.close()
    # parse the xml file listing the allocations
    etree = xml.etree.ElementTree.parse(args.requests).getroot()


    def process_path(path, size):
        """
        Compare the disk usage with the allocation
        """
        name = basename(path)
        size_tb = float(size) / 1024.0**4
        messages = []

        disks = etree.findall("disk[dirname='%s']"%name)
        print(type(etree))

        if len(disks) > 1:
            messages.append('Multiple allocations for this directory')

        if len(disks) == 0:
            expire_date = datetime.min
            quota_tb = 0
            old_size_tb = 0.
            size_diff = 0.
            contact = ''
            managers = ''
            messages.append('No allocation')
        else:
            quota_tb = float(disks[0].find('allocation').text)   or 0.0
            expire_date = datetime.strptime(disks[0].find('expire').text, "%Y-%m-%d") or ''
            managers = disks[0].find('managers').text or ''
            disk_id = disks[0].find('id').text or ''
            old_size_tb = float(disks[0].find('last_size').text)  or 0.0
            size_diff = size_tb - old_size_tb
            contact = disks[0].find('contact').text or ''
            json_url='http://144.6.225.151:3000/api/v0/disks/' + disk_id + '/update.json'
            check_date = date.today().strftime("%Y-%m-%d")
            post_json(json_url, {'size':str(size_tb), 'last_check': check_date})

        if size_tb > quota_tb:
            messages.append('Over quota')

        if expire_date > datetime.now():
            messages.append('Allocation expired')

        return [name, managers, quota_tb, size_tb, size_diff, contact, ', '.join(messages)]

    print(tabulate(
         [process_path(path, size) for path, size in q],
         headers = ['Project', 'Managers', 'Quota (Tb)', 'Size (Tb)', 'Diff (Tb)', 'Contact', 'Warnings']))

if __name__ == '__main__':
    main()
