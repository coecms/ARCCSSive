CMIP5
=====

.. module:: ARCCSSive.CMIP5

The CMIP5 module provides tools for searching through the CMIP5 data stored on NCI's `/g/data` filesystem

Getting Started:
----------------

The ARCCSSive library is available as a module on Raijin. Load it using::

    module use ~access/modules
    module load pythonlib/ARCCSSive

To use the CMIP5 catalog you first need to connect to it::

    from ARCCSSive import CMIP5
    cmip5 = CMIP5.connect()

The session object allows you to run queries on the catalog. There are a number
of helper functions for common operations, for instance searching through the
model outputs::

    outputs = cmip5.outputs(
        experiment = 'rcp45',
        variable   = 'tas',
        mip        = 'Amon')

You can then loop over the search results in normal Python fashion::

    for o in outputs:
        print o.model, o.filenames()

Examples
--------

Get files from a single model variable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    from ARCCSSive import CMIP5
    cmip5 = CMIP5.connect()

    outputs = cmip5.outputs(
        experiment = 'rcp45',
        variable   = 'tas',
        mip        = 'Amon',
        model      = 'ACCESS1-3',
        ensemble   = 'r1i1p1')

    for f in outputs.first().filenames():
        print f


Get files from all models for a specific variable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    from ARCCSSive import CMIP5
    cmip5 = CMIP5.connect()

    outputs = cmip5.outputs(
        experiment = 'rcp45',
        variable   = 'tas',
        mip        = 'Amon',
        ensemble   = 'r1i1p1')

    for m in outputs:
        model = m.model
        files = m.filenames()

Choose more than one variable at a time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

More complex queries on the :func:`Session.outputs()` results can be performed using
SQLalchemy's `filter() <http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#common-filter-operators>`_::

    from ARCCSSive import CMIP5
    from ARCCSSive.CMIP5.Model import *
    from sqlalchemy import *

    cmip5 = CMIP5.connect()

    outputs = cmip5.outputs(
        experiment = 'rcp45',
        model      = 'ACCESS1-3',
        mip        = 'Amon',) \
        .filter(Variable.variable.in_(['tas','pr']))

Get results from a specific output version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Querying specific versions currently needs to go through the
:func:`Session.query()` function, this will be simplified in a future version of
ARCCSSive::

    from ARCCSSive import CMIP5
    from ARCCSSive.CMIP5.Model import *

    outputs = cmip5.versions(
    res = cmip5.query(Version) \
            .join(Variable) \
            .filter_by(
        model      = 'ACCESS1-3',
        version    = 'v20120413',
        experiment = 'rcp45',
        mip        = 'Amon',
        ensemble   = 'r1i1p1')

    # This returns a sequence of Version, get the variable information from the
    # .variable property
    for o in res:
        print print o.variable.model, o.variable.variable, o.filenames()

Compare model results between two experiments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Link two sets of outputs together using joins::


    from ARCCSSive import CMIP5
    from ARCCSSive.CMIP5.Model import *
    from sqlalchemy.orm import aliased
    from sqlalchemy import *

    cmip5 = CMIP5.connect()

    # Create aliases for the historical and rcp variables, so we can
    distinguish them in the query
    histVariable = aliased(Variable)
    rcpVariable  = aliased(Variable)
    rcp_hist  = cmip5.query(rcpVariable, histVariable).join(
            histVariable, and_(
                histVariable.variable == rcpVariable.variable,
                histVariable.model    == rcpVariable.model,
                histVariable.mip      == rcpVariable.mip,
                histVariable.ensemble == rcpVariable.ensemble,
            )).filter(
                rcpVariable.experiment  == 'rcp45',
                histVariable.experiment == 'historicalNat',
            )

    for r, h in rcp_hist:
        print r.versions[-1].path, h.versions[-1].path

API
---

connect()
~~~~~~~~~
.. autofunction:: connect()

Session
~~~~~~~

The session object has a number of helper functions for getting information out
of the catalog, e.g. :func:`Session.models()` gets a list of all available
models.

.. autoclass:: Session
    :members:

Model
~~~~~

.. module:: ARCCSSive.CMIP5.Model

The model classes hold catalog information for a single entry. Each model run
variable can have a number of different data versions, as errors get corrected
by the publisher, and each version can consist of a number of files split into
a time sequence.

.. autoclass:: Variable
    :members:
    :member-order: bysource

.. autoclass:: Version
    :members:
    :member-order: bysource
