import pytest
import sys

collect_ignore = ["setup.py", "docs/conf.py"]

# Pyesgf doesn't work with python 3
if sys.version_info >= (3,0):
    collect_ignore.append('ARCCSSive/CMIP5/pyesgf_functions.py')
    collect_ignore.append('ARCCSSive/CMIP5/compare_helpers.py')

from ARCCSSive.db import *

@pytest.fixture(scope='session')
def database():
    engine = connect(echo=True)
    engine.connect()

    yield Session

@pytest.fixture()
def session(database):
    """
    A sqlalchemy session
    """
    s = database()
    yield s
    s.rollback()

import ARCCSSive.CMIP5
@pytest.fixture()
def CMIP5_session(session):
    """
    A CMIP5 session
    """
    connection = ARCCSSive.CMIP5.Session()
    connection.session = session
    yield connection
