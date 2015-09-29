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

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from Model import Base, Version, Variable, File

Session = sessionmaker()

class CMIP5Session():
    """Holds a connection to the catalog
    """

    def query(self, *args, **kwargs):
        """Query the CMIP5 catalog

        Allows you to filter the full list of CMIP5 outputs using `SQLAlchemy commands <http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying>`_

        :return: A SQLalchemy query object
        """
        return self.session.query(*args, **kwargs)

    def latest_variables(self):
        """ Returns the most recent version of each variable

        :return: An iterable returning pairs of
            :py:class:`Model.Variable`,
            :py:class:`Model.Version` where the Version
            is the most recent matching that variable
        """
        sub = self.query(Version.variable_id, Version.version, Version.id, func.max(Version.version)).group_by(Version.variable_id).subquery()
        return self.query(Variable, sub.c.version).filter(Variable.id == sub.c.variable_id)

    def files(self, 
            startYear=None, 
            endYear=None, 
            **kwargs):
        """ Query the list of files

        Returns a list of files that match the arguments

        :argument model:
        :argument experiment:
        :argument variable:
        :argument mip:
        :argument ensemble:
        :argument startYear: Only files with data after this year
        :argument endYear: Only files with data before this year

        :return: An iterable returning :py:class:`Model.File`
            matching the search query
        """
        latest = self.latest_variable_versions().filter_by(**kwargs).subquery()
        return self.query(File.path).filter(File.version_id == latest.c.id)

    def models(self):
        """ Get the list of all models in the dataset
        """
        return self.query(Variable.model).distinct().all()

    def experiments(self):
        """ Get the list of all experiments in the dataset
        """
        return self.query(Variable.experiment).distinct().all()

    def variables(self):
        """ Get the list of all variables in the dataset
        """
        return self.query(Variable.variable).distinct().all()

    def mips(self):
        """ Get the list of all MIP tables in the dataset
        """
        return self.query(Variable.mip).distinct().all()


def connect(path = 'sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db'):
    """Connect to the CMIP5 catalog

    :return: A new :py:class:`CMIP5Session`

    Example::

        from ARCCSSive import CMIP5
        session = CMIP5.DB.connect()
        outputs = session.query()
    """
    engine = create_engine(path)
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)

    connection = CMIP5Session()
    connection.session = Session()
    return connection

