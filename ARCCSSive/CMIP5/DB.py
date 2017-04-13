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

from __future__ import print_function

import os

from sqlalchemy import create_engine, func, select, and_
from sqlalchemy.orm import sessionmaker

from ARCCSSive.CMIP5.Model import Base, Instance

SQASession = sessionmaker()

class Session(object):
    """Holds a connection to the catalog

    Create using :func:`ARCCSSive.CMIP5.connect()`
    """

    def query(self, *args, **kwargs):
        """Query the CMIP5 catalog

        Allows you to filter the full list of CMIP5 outputs using `SQLAlchemy commands <http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying>`_

        :return: A SQLalchemy query object
        """
        return self.session.query(*args, **kwargs)

    def files(self, **kwargs):
        """ Query the list of files

        Returns a list of files that match the arguments

        :argument **kwargs: Match any attribute in :class:`Model.Instance`, e.g. `model = 'ACCESS1-3'`

        :return: An iterable returning :py:class:`Model.File`
            matching the search query
        """
        raise NotImplementedError

    def models(self):
        """ Get the list of all models in the dataset

        :return: A list of strings
        """
        return [x[0] for x in self.query(Instance.model).distinct().all()]

    def experiments(self):
        """ Get the list of all experiments in the dataset

        :return: A list of strings
        """
        return [x[0] for x in self.query(Instance.experiment).distinct().all()]

    def variables(self):
        """ Get the list of all variables in the dataset

        :return: A list of strings
        """
        return [x[0] for x in self.query(Instance.variable).distinct().all()]

    def mips(self):
        """ Get the list of all MIP tables in the dataset

        :return: A list of strings
        """
        return [x[0] for x in self.query(Instance.mip).distinct().all()]

    def outputs(self, **kwargs):
        """ Get the most recent instances matching a query

        Arguments are optional, using them will select only matching outputs

        :argument variable: CMIP variable name
        :argument experiment: CMIP experiment
        :argument mip: MIP table
        :argument model: Model used to generate the dataset
        :argument ensemble: Ensemble member

        :return: An iterable sequence of :class:`ARCCSSive.CMIP5.Model.Instance`
        """
        return self.query(Instance).filter_by(**kwargs)

# Default CMIP5 database
default_db = 'sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db'

def connect(path = None):
    """Connect to the CMIP5 catalog

    :return: A new :py:class:`Session`

    Example::

    >>> from ARCCSSive import CMIP5 
    >>> cmip5   = CMIP5.DB.connect() # doctest: +SKIP
    >>> outputs = cmip5.query() # doctest: +SKIP
    """

    if path is None:
        # Get the path from the environment
        path = os.environ.get('CMIP5_DB', default_db)

    engine = create_engine(path)
    Base.metadata.create_all(engine)
    SQASession.configure(bind=engine, autoflush=False)

    connection = Session()
    connection.session = SQASession()
    return connection

