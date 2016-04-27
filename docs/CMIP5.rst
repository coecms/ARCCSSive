CMIP5
=====

.. default-domain:: py
.. module:: ARCCSSive.CMIP5
.. testsetup::

    >>> import six

The CMIP5 module provides tools for searching through the CMIP5 data stored on NCI's `/g/data` filesystem

Getting Started:
----------------

The ARCCSSive library is available as a module on Raijin. Load it using::

    module use ~access/modules
    module load pythonlib/ARCCSSive

To use the CMIP5 catalog you first need to connect to it::

    >>> from ARCCSSive import CMIP5 
    >>> cmip5 = CMIP5.connect() # doctest: +SKIP

.. testsetup::

    >>> # Get a fixture to show examples
    >>> cmip5 = getfixture('session')

The session object allows you to run queries on the catalog. There are a number
of helper functions for common operations, for instance searching through the
model outputs:

.. doctest::

    >>> outputs = cmip5.outputs(
    ...     experiment = 'rcp45',
    ...     variable   = 'tas',
    ...     mip        = 'Amon')

You can then loop over the search results in normal Python fashion::

    >>> for o in outputs:
    ...     six.print_(o.model, *o.filenames())
    ACCESS1-3 example.nc

Examples
--------

Get files from a single model variable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. doctest::

    >>> outputs = cmip5.outputs(
    ...     experiment = 'rcp45',
    ...     variable   = 'tas',
    ...     mip        = 'Amon',
    ...     model      = 'ACCESS1-3',
    ...     ensemble   = 'r1i1p1')

    >>> for f in outputs.first().filenames():
    ...     six.print_(f)
    example.nc


Get files from all models for a specific variable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. doctest::

    >>> outputs = cmip5.outputs(
    ...     experiment = 'rcp45',
    ...     variable   = 'tas',
    ...     mip        = 'Amon',
    ...     ensemble   = 'r1i1p1')

    >>> for m in outputs:
    ...     model = m.model
    ...     files = m.filenames()

Choose more than one variable at a time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

More complex queries on the :func:`Session.outputs()` results can be performed using
SQLalchemy's `filter() <http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#common-filter-operators>`_:

.. doctest::

    >>> from ARCCSSive.CMIP5.Model import *
    >>> from sqlalchemy import *

    >>> outputs = cmip5.outputs(
    ...     experiment = 'rcp45',
    ...     model      = 'ACCESS1-3',
    ...     mip        = 'Amon',) \
    ...     .filter(Instance.variable.in_(['tas','pr']))

Get results from a specific output version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Querying specific versions currently needs to go through the
:func:`Session.query()` function, this will be simplified in a future version of
ARCCSSive:

.. doctest::

    >>> from ARCCSSive.CMIP5.Model import *

    >>> res = cmip5.query(Version) \
    ...         .join(Instance) \
    ...         .filter(
    ...     Version.version     == 'v20120413',
    ...     Instance.model      == 'ACCESS1-3',
    ...     Instance.experiment == 'rcp45',
    ...     Instance.mip        == 'Amon',
    ...     Instance.ensemble   == 'r1i1p1')

    >>> # This returns a sequence of Version, get the variable information from
    >>> # the .variable property
    >>> for o in res:
    ...     six.print_(o.variable.model, o.variable.variable, o.filenames())

Compare model results between two experiments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Link two sets of outputs together using joins:

.. doctest::


    >>> from ARCCSSive.CMIP5.Model import *
    >>> from sqlalchemy.orm import aliased
    >>> from sqlalchemy import *

    >>> # Create aliases for the historical and rcp variables, so we can
    >>> # distinguish them in the query
    >>> histInstance = aliased(Instance)
    >>> rcpInstance  = aliased(Instance)
    >>> rcp_hist  = cmip5.query(rcpInstance, histInstance).join(
    ...         histInstance, and_(
    ...             histInstance.variable == rcpInstance.variable,
    ...             histInstance.model    == rcpInstance.model,
    ...             histInstance.mip      == rcpInstance.mip,
    ...             histInstance.ensemble == rcpInstance.ensemble,
    ...         )).filter(
    ...             rcpInstance.experiment  == 'rcp45',
    ...             histInstance.experiment == 'historicalNat',
    ...         )

    >>> for r, h in rcp_hist:
    ...     six.print_(r.versions[-1].path, h.versions[-1].path)

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

.. autoclass:: Instance
    :members:
    :member-order: bysource

.. autoclass:: Version
    :members:
    :member-order: bysource
