==============
Administration
==============

---
Making a new release
---

 1. Use the Github interface to create a new relase with the version number,
     e.g. '1.2.3'. This should use semantic versioning, if it's a minor change
     increase the third number, if it introduces new features increase the
     second number and if it will break existing scripts using the library
     increase the first number.

 2. This will cause Travis to build the library and upload it to PyPI
