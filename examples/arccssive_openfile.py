#!/usr/bin/env python
# This is an example of how to use the ARCCSSive module in a python script to search CMIP5, open files to read data and calculate something.
# Uses ARCCSSive module to find data and netCDF4 to open files
# Last modified:
#             2017-02-27 
"""
Copyright 2016 ARC Centre of Excellence for Climate Systems Science

author: Paola Petrelli <paola.petrelli@utas.edu.au>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import print_function

from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.Model import Instance 
import numpy as np
from netCDF4 import MFDataset

# define a function that open the files, read the variable and return max
def var_max(var,path):
    ''' calculate max value for variable '''
    # open ensemble files as one aggregated file
    # using path+"/*.nc" should be sufficient, unfortunately GISS models break conventions and they put all variables in one directory
    #  so I'm using "/"+var+"_*.nc" just in case, otherwise you can save the drstree_path instead, although if updated recently this might not work
    print(path+"/"+var+"_*.nc")
    nc=MFDataset(path+"/*.nc",'r')
    #nc=MFDataset(path+"/"+var+"_*.nc",'r')
    data = nc.variables[var][:]
    return np.max(data)

# step1: connect to the database
db=CMIP5.connect()

# step2: search database without specifying variables and then use filter to select only two
outputs=db.outputs(model='CanESM2',experiment='historical',mip='Amon',ensemble='r1i1p1')\
        .filter(Instance.variable.in_(['tas','pr']))

# Optional: loop through result instance objects returned by search and print some information
print("Search found %s instances" % outputs.count())
#for o in outputs:
#    var = o.variable
#    print(str(var))
#    files = o.filenames()
#    print(files)
#    fpath = o.drstree_path()
#    print(str(fpath))
# loops through result version objects related to instance
#    for v in o.versions:
# print message if version is latest on ESGF
#        if v.is_latest: print("latest available version on ESGF as of ",str(v.checked_on))
# print checksum and tracking-id for first file listed
#    print(str(o.versions[0].files[0].sha256))
#    print(str(o.versions[0].files[0].md5))
#    print(str(o.versions[0].files[0].tracking_id))

# step3: get the list of models returned by search and calculate something for each of the available ensembles.

for o in outputs:
    var = o.variable
    for v in o.versions:
        varmax=var_max(var,v.path)
        print("Maximum value for variable %s, version % is %" % var, v.version, varmax)
