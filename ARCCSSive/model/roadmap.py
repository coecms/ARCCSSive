#!/usr/bin/env python
# Copyright 2017 ARC Centre of Excellence for Climate Systems Science
# author: Paola Petrelli <paola.petrelli@utas.edu.au>
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
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker


class Disks(object):
    pass
 
def loadSession(debug):
    """"""    
    f=open('/home/581/pxp581/.roadmap')
    db_user, db_psswrd = f.readlines()[0][:-1].split(" ")
    f.close()
    engine = create_engine('mysql+pymysql://'+ db_user + ":" + db_psswrd + '@144.6.225.37/roadmapdev', echo = debug)
    metadata = MetaData(engine)
    disks = Table('disks', metadata, autoload=True)
    mapper(Disks, disks)
 
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
 
if __name__ == "__main__":
    session = loadSession()

