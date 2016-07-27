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

or to install the current development version (note this uses a different
database):

    vdi $ pip install --user git+https://github.com/coecms/ARCCSSive.git 
    vdi $ export CMIP5_DB=sqlite:////g/data1/ua6/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db

Once the library is installed run `ipython notebook` to start a new notebook
