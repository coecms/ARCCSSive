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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from Model import Base, CMIP5Output

Session = sessionmaker()

class CMIP5Session():
    """Holds a SQLAlchemy session to allow for queries
    """

    def query(self):
        """Query the CMIP5 database

        Allows you to filter the full list of CMIP5 outputs using SQLAlchemy

        Returns a interable list of ModelOutputs

        Example:
            
            from ARCCSSive import CMIP5
            for result in CMIP5.query().filter_by(institute='CSIRO'):
                print result.model, result.experiment
        """
        return self.session.query(CMIP5Output)

def connect():
    """Initialise the DB session

    Example:
        session = CMIP5.DB.connect()
        outputs = session.query()
    """
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)

    connection = CMIP5Session()
    connection.session = Session()
    return connection

