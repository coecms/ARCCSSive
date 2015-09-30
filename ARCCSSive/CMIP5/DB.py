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

from sqlalchemy import create_engine, func, select, join, and_
from sqlalchemy.orm import sessionmaker

from Model import Base, Version, Variable, Latest

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

    def files(self, **kwargs):
        """ Query the list of files

        Returns a list of files that match the arguments

        :argument **kwargs: Match any attribute in :class:`Model.Variable`, e.g. `model = 'ACCESS1-3'`

        :return: An iterable returning :py:class:`Model.File`
            matching the search query
        """
        pass

    def models(self):
        """ Get the list of all models in the dataset
        """
        return [x[0] for x in self.query(Variable.model).distinct().all()]

    def experiments(self):
        """ Get the list of all experiments in the dataset
        """
        return [x[0] for x in self.query(Variable.experiment).distinct().all()]

    def variables(self):
        """ Get the list of all variables in the dataset
        """
        return [x[0] for x in self.query(Variable.variable).distinct().all()]

    def mips(self):
        """ Get the list of all MIP tables in the dataset
        """
        return [x[0] for x in self.query(Variable.mip).distinct().all()]

    def all_outputs(self):
        """ Return all output variables
        """
        return self.query(Variable)

    def latest_output_versions(self):
        """ Returns the most recent version of each variable

        :return: An iterable returning pairs of
            :py:class:`Model.Variable`,
            :py:class:`Model.Version` where the Version
            is the most recent matching that variable
        """
        sub = self.query(Version.variable_id, Version.version, Version.id, func.max(Version.version)).group_by(Version.variable_id).subquery()
        return self.query(Variable, Version) \
            .filter(Variable.id == sub.c.variable_id) \
            .filter(Version.id == sub.c.id)

    def outputs(self):
        """ Get the most recent files matching a query
        """
        return [ver for var, ver in self.latest_output_versions()]


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

def update(session):
    """ Update the tables using the latest values
    """
    conn = session.connection()

    variable = Variable.__table__
    version  = Version.__table__
    latest   = Latest.__table__

    # Add any missing variables to the 'variable' table

    # A list of ids for de-duplicated variables
    unique = select([func.min(latest.c.id)]).group_by(
        latest.c.variable,   
        latest.c.experiment, 
        latest.c.mip,        
        latest.c.model,      
        latest.c.ensemble,   
        )
    # A list of values already in the variables table
    j = join(latest, variable, and_(
        variable.c.variable   == latest.c.variable,
        variable.c.experiment == latest.c.experiment,
        variable.c.mip        == latest.c.mip,
        variable.c.model      == latest.c.model,
        variable.c.ensemble   == latest.c.ensemble,
        ), isouter=True)
    missing = select([
        latest.c.variable,   
        latest.c.experiment, 
        latest.c.mip,        
        latest.c.model,      
        latest.c.ensemble,   
        ]).where(latest.c.id.in_(unique)).select_from(j).where(variable.c.variable.is_(None))
    insert     = variable.insert().from_select(['variable','experiment','mip','model','ensemble'], missing)

    print insert
    print
    conn.execute(insert)

    # Add any missing versions to the 'version' table, linking with the releant variable

    j = join(latest, variable, and_(
        variable.c.variable   == latest.c.variable,
        variable.c.experiment == latest.c.experiment,
        variable.c.mip        == latest.c.mip,
        variable.c.model      == latest.c.model,
        variable.c.ensemble   == latest.c.ensemble,
        ))
    latest_ids = select([version.c.latest_id])
    missing    = select([latest.c.id, latest.c.path, latest.c.version, variable.c.id]).select_from(j).where(latest.c.id.notin_(latest_ids))
    insert     = version.insert().from_select(['latest_id','path','version','variable_id'], missing)

    print insert
    print
    conn.execute(insert)
