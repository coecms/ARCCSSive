import pytest
import sys

# Make session fixture available to doctests
from tests.CMIP5.db_fixture import session

collect_ignore = ["setup.py", "docs/conf.py"]

# Pyesgf doesn't work with python 3
if sys.version_info >= (3,0):
    collect_ignore.append('ARCCSSive/CMIP5/pyesgf_functions.py')
    collect_ignore.append('ARCCSSive/CMIP5/compare_helpers.py')
    collect_ignore.append('ARCCSSive/cli/compare_ESGF.py')
