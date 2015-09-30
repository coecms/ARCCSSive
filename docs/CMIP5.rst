CMIP5
=====

.. module:: ARCCSSive.CMIP5

The CMIP5 module provides tools for searching through the CMIP5 data stored on NCI's `/g/data` filesystem

Getting Started:
----------------

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
        print o.model, o.files()

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

    for f in outputs.first().files():
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
        files = m.files()

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
