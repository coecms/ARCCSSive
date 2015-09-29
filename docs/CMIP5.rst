CMIP5
=====

.. module:: ARCCSSive.CMIP5

The CMIP5 module provides tools for searching through the CMIP5 data stored on NCI's `/g/data` filesystem

Getting Started:
----------------

To use the CMIP5 catalog you first need to connect to it::

    from ARCCSSive import CMIP5
    session = CMIP5.connect()

The session object allows you to run queries on the catalog. There are a number
of helper functions for common operations, for instance retrieving a list of files::

    files = session.files(
        experiment = 'rcp45',
        variable   = 'tas',
        mip        = 'Amon')

You can then loop over the search results in normal Python fashion::

    for f in files:
        print f.path

connect()
---------
.. autofunction:: connect()

CMIP5Session
------------

.. autoclass:: CMIP5Session
    :members:

Model
-----

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

.. autoclass:: File
    :members:
    :member-order: bysource
