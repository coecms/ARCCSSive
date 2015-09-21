# ARCCSSive
ARCCSS Data Access Tools

[![Documentation Status](https://readthedocs.org/projects/arccssive/badge/?version=latest)](https://readthedocs.org/projects/arccssive/?badge=latest)
[![Build Status](https://travis-ci.org/coecms/ARCCSSive.svg?branch=master)](https://travis-ci.org/coecms/ARCCSSive)
[![codecov.io](http://codecov.io/github/coecms/ARCCSSive/coverage.svg?branch=master)](http://codecov.io/github/coecms/ARCCSSive?branch=master)
[![Code Climate](https://codeclimate.com/github/coecms/ARCCSSive/badges/gpa.svg)](https://codeclimate.com/github/coecms/ARCCSSive)

For full documentation please see http://arccssive.readthedocs.org

CMIP5
=====

Query and access the CMIP5 data from Raijin

```
from ARCCSSive import CMIP5

session = CMIP5.DB.connect()
for output in session.query().filter_by(model='ACCESS1-0'):
    data = output.dataset()    
```

Uses [SQLAlchemy](http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying) to filter and sort the data files. Retrieve xray-aggregated time series using the `dataset()` method.
