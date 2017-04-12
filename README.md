# ARCCSSive
ARCCSS Data Access Tools

[![Documentation Status](https://readthedocs.org/projects/arccssive/badge/?version=latest)](https://readthedocs.org/projects/arccssive/?badge=latest)
[![Build Status](https://travis-ci.org/coecms/ARCCSSive.svg?branch=master)](https://travis-ci.org/coecms/ARCCSSive)
[![CircleCI](https://circleci.com/gh/coecms/ARCCSSive.svg?style=shield)](https://circleci.com/gh/coecms/ARCCSSive)
[![codecov.io](http://codecov.io/github/coecms/ARCCSSive/coverage.svg?branch=master)](http://codecov.io/github/coecms/ARCCSSive?branch=master)
[![Code Health](https://landscape.io/github/coecms/ARCCSSive/master/landscape.svg?style=flat)](https://landscape.io/github/coecms/ARCCSSive/master)
[![Code Climate](https://codeclimate.com/github/coecms/ARCCSSive/badges/gpa.svg)](https://codeclimate.com/github/coecms/ARCCSSive)
[![PyPI version](https://badge.fury.io/py/ARCCSSive.svg)](https://pypi.python.org/pypi/ARCCSSive)
[![Anaconda-Server Badge](https://anaconda.org/coecms/arccssive/badges/version.svg)](https://anaconda.org/coecms/arccssive)

For full documentation please see http://arccssive.readthedocs.org/en/stable

Installing
==========

### Raijin

The stable version of ARCCSSive is available on Rajin in the `analysis27` Anaconda environment:

    raijin $ module use /g/data3/hh5/public/modules
    raijin $ module load conda/analysis27

and is also available as a module:

    raijin $ module use ~access/modules
    raijin $ module load pythonlib/ARCCSSive

### NCI Virtual Desktops

NCI's virtual desktops allow you to use ARCCSSive from a Jupyter notebook. For
details on how to use virtual desktops see http://vdi.nci.org.au/help

ARCCSSive can be accessed on VDI using the Anaconda environments:

    vdi $ module use /g/data3/hh5/public/modules
    vdi $ module load conda/analysis27

### Local Install

You can install ARCCSSive locally using either Anaconda or Pip. You will need
to copy the database file from Raijin

    $ pip install ARCCSSive
    # or
    $ conda install -c coecms arccssive

    $ scp raijin:/g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db $PWD/cmip5.db
    $ export CMIP5_DB=sqlite:///$PWD/cmip5.db

### Development Version

To install the current development version with a test database:

    $ pip install --user git+https://github.com/coecms/ARCCSSive.git 
    $ export CMIP5_DB=sqlite:///$HOME/cmip5.db

CMIP5
=====

Query and access the CMIP5 data from Raijin

```python
from ARCCSSive import CMIP5

cmip = CMIP5.DB.connect()
for output in cmip.outputs(model='ACCESS1-0'):
    variable = output.variable
    files    = output.filenames()    
```

Uses
[SQLAlchemy](http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying)
to filter and sort the data files.
