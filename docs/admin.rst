==============
Administration
==============

---
Making a new release
---

Use the Github interface to create a new relase with the version number,
e.g. '1.2.3'. This should use semantic versioning, if it's a minor change
increase the third number, if it introduces new features increase the
second number and if it will break existing scripts using the library
increase the first number.

After doing this the following will happen:

 * Travis-ci will upload the package to PyPI

 * CircleCI will upload the package to Anaconda

 * The conda update cron job at NCI will pick up the new version overnight
