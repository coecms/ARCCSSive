#!/usr/bin/env python

from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.Model import *
from ARCCSSive.CMIP5.insert import get_or_insert
from sqlalchemy.sql import exists

session = CMIP5.connect('sqlite:///latest.db')

# Get list of versions already present
present = exists().where(Latest.id == Version.path)
unmatched = session.query(Latest).filter(~present)

print unmatched
# for u in unmatched:
#     version = get_or_insert(session.session,
#             Version,
#             version = u.version,
#             path    = u.id)
#     print 'Added ',version.path

missing_var = session.query(Version, Latest).filter(Version.path == Latest.id).filter(Version.variable_id == None)
print missing_var
# for v, l in missing_var:
#     variable = get_or_insert(session.session,
#             Variable,
#             model      = file.model,
#             experiment = file.experiment,
#             variable   = file.variable,
#             mip        = file.mip,
#             ensemble   = file.ensemble)
#     v.variable_id = variable.id

