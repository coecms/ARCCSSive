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

import Model

Session = sessionmaker()

class DBSession():
    """Holds a connection to the catalog
    """

    def query(self, *args, **kwargs):
        """Query the CMIP5 database

        Allows you to filter the full list of CMIP5 outputs using SQLAlchemy
        commands

        :return: a interable sequence of :py:class:`Dataset`

        Example::
            
            from ARCCSSive import CMIP5
            session = CMIP5.DB.connect()

            # Filter using SQLAlchemy on the DRS values
            for result in session.query().filter_by(institute='CSIRO'):

                # Get values from the DRS using attributes
                print result.model, result.experiment

                # Get a xray Dataset combining all timeslices in this output
                data = result.dataset()
        """
        return self.session.query(*args, **kwargs)

    def variables(self):
        return self.query(Model.File.variable).distinct().all()
    def mips(self):
        return self.query(Model.File.mip).distinct().all()
    def models(self):
        return self.query(Model.File.model).distinct().all()
    def experiments(self):
        return self.query(Model.File.experiments).distinct().all()
    def files(self):
        return self.query(Model.File)

def connect(path = 'sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db'):
    """Initialise the DB session

    :return: A :py:class:`ARCCSSive.CMIP5.DB.CMIP5Session`

    Example::

        from ARCCSSive import CMIP5
        session = CMIP5.DB.connect()
        outputs = session.query()
    """
    engine = create_engine(path)
    Model.Base.metadata.create_all(engine)
    Session.configure(bind=engine)

    connection = DBSession()
    connection.session = Session()
    return connection

