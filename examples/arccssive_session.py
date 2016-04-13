# coding: utf-8
from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.Model import *
db=CMIP5.connect()
outputs=db.outputs(variable='tas',model='MIROC5',experiment='historical',mip='Amon',ensemble='r1i1p1')

for o in outputs:
        model = o.model
        print model 
        files = o.filenames()
        print files 
        fpath = o.drstree_path()
        print fpath
        for v in o.versions:
            if v.is_latest: print "latest available version on ESGF as of ",v.checked_on


outputs=db.outputs(model='MIROC5',experiment='historical',mip='Amon',ensemble='r1i1p1')\
              .filter(Instance.variable.in_(['tas','pr']))
for o in outputs:
        var = o.variable
        print var
        files = o.filenames()
        print files
        fpath = o.drstree_path()
        print fpath
        for v in o.versions:
            if v.is_latest: print "latest available version on ESGF as of ",v.checked_on
        print o.versions[0].files[0].sha256
