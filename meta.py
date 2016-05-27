#!/usr/bin/env python
"""
Copyright 2015 ARC Centre of Excellence for Climate Systems Science

author: Scott Wales <scott.wales@unimelb.edu.au>

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

from subprocess import Popen, PIPE

def git_describe():
    proc = Popen(['git','describe','--tags','--always','--long'],stdout=PIPE)
    desc  = proc.communicate()[0].decode('utf-8').strip()
    ihash  = desc.rindex('-')
    ichange = desc.rindex('-',0,ihash)
    return (desc[0:ichange],desc[ichange+1:ihash],desc[ihash+1:-1])

def pep440():
    tag, changes, hash = git_describe()
    if changes == '0':
        return tag
    else:
        return "%s.dev%s+%s"%(tag,changes,hash)

version = pep440()
release,_,_ = git_describe()
