# ARCCSSive
ARCCSS Data Access Tools

[![Documentation Status](https://readthedocs.org/projects/arccssive/badge/?version=latest)](https://readthedocs.org/projects/arccssive/?badge=latest)
[![Build Status](https://travis-ci.org/coecms/ARCCSSive.svg?branch=master)](https://travis-ci.org/coecms/ARCCSSive)
[![codecov.io](http://codecov.io/github/coecms/ARCCSSive/coverage.svg?branch=master)](http://codecov.io/github/coecms/ARCCSSive?branch=master)
[![Code Health](https://landscape.io/github/coecms/ARCCSSive/master/landscape.svg?style=flat)](https://landscape.io/github/coecms/ARCCSSive/master)
[![Code Climate](https://codeclimate.com/github/coecms/ARCCSSive/badges/gpa.svg)](https://codeclimate.com/github/coecms/ARCCSSive)
[![PyPI version](https://badge.fury.io/py/ARCCSSive.svg)](https://pypi.python.org/pypi/ARCCSSive)

For full documentation please see http://arccssive.readthedocs.org/en/stable

Installing
==========

### Raijin

The stable version of ARCCSSive is available as a module on NCI's Raijin supercomputer:

    raijin $ module use ~access/modules
    raijin $ module load pythonlib/ARCCSSive

### NCI Virtual Desktops

NCI's virtual desktops allow you to use ARCCSSive from a Jupyter notebook. For
details on how to use virtual desktops see http://vdi.nci.org.au/help

To install the stable version of ARCCSSive:

    vdi $ pip install --user ARCCSSive
    vdi $ export CMIP5_DB=sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db

or to install the current development version:

    vdi $ pip install --user git+https://github.com/coecms/ARCCSSive.git 
    vdi $ export CMIP5_DB=sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db

Once the library is installed run `ipython notebook` to start a new notebook

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
