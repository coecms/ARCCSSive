#!/usr/bin/env python
# Copyright 2017 ARC Centre of Excellence for Climate Systems Science
# author: Scott Wales <scott.wales@unimelb.edu.au>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function
from ARCCSSive.CMIP5.Model import *
from ARCCSSive.CMIP5 import connect
from tabulate import tabulate
import sys
import os.path
import sqlalchemy.sql.functions as sqlf
import sqlalchemy.sql.expression as sqle

import click

@click.group()
def main():
    """
    ARCCSS Data Catalogue Tools
    """
    pass

@main.group()
def cmip5():
    """
    CMIP5 Catalogue Search
    """
    pass

_select_options = [
    click.option('-m','--model', multiple=True, help="CMIP Model (can be set multiple times for OR)"),
    click.option('-e','--experiment', multiple=True, help="CMIP Experiment (\")"),
    click.option('-v','--variable', multiple=True, help="CMIP Variable (\")"),
    click.option('-t','--mip', multiple=True, help="MIP table (\")"),
    click.option('-n','--ensemble', multiple=True, help="Dataset ensemble (\")"),
    click.option('-V','--version', multiple=True, help="Dataset version (\")"),
    click.option('--latest/--no-latest', default=False, help="Search latest version only"),
    click.option('--debug/--no-debug', default=False, help="Show SQL query"),
    ]

def select_options(func):
    # Common selection & filtering options
    # Apply in reverse order for correct help text
    for option in reversed(_select_options):
        func = option(func)
    return func

def select(q, model, experiment, variable, mip, 
        ensemble, version, latest, debug, session):
    """
    Filter the results (Reqires query containing Instance and Version)
    """

    if latest:
        # Get the maximum version (string ordering) for each instance, omitting 'NA' values
        s = (session.query(Version.instance_id, sqlf.max(Version.version).label('latest_version'))
                .filter(Version.version != 'NA').group_by(Version.instance_id).subquery())
        q = q.join(s, sqle.and_(Version.instance_id == s.c.instance_id, Version.version == s.c.latest_version))

    # Apply common filtering to a query
    if len(model) > 0:
        q = q.filter(Instance.model.in_(model))
    if len(experiment) > 0:
        q = q.filter(Instance.experiment.in_(experiment))
    if len(variable) > 0:
        q = q.filter(Instance.variable.in_(variable))
    if len(mip) > 0:
        q = q.filter(Instance.mip.in_(mip))
    if len(ensemble) > 0:
        q = q.filter(Instance.ensemble.in_(ensemble))
    if len(version) > 0:
        q = q.filter(Version.version.in_(version))

    if debug:
        print(q, file=sys.stderr)
    return q

@cmip5.command()
@select_options
def search(model, experiment, variable, mip, 
        ensemble, version, latest, debug):
    """
    Search local CMIP5 datasets
    """
    cmip5 = connect()
    
    q = cmip5.query(Instance, Version.version).join(Version)
    q = select(q, model, experiment, variable, mip, 
        ensemble, version, latest, debug, cmip5)
    tabulate_instances(q)

def tabulate_instances(instances):
    """
    Print a table of instance information
    """
    headers= ['model', 'experiment', 'mip', 'variable', 'ensemble', 'version']
    table = [[r.model, r.experiment, r.mip, r.variable, r.ensemble, v] 
            for r, v in instances.limit(50).all()]
    print(tabulate(table, headers=headers))

@cmip5.command()
@select_options
def files(model, experiment, variable, mip, 
        ensemble, version, latest, debug):
    """
    Search local CMIP5 files
    """
    cmip5 = connect()
    
    q = (cmip5.query(VersionFile.filename, Version.path)
            .join(Version).join(Instance))
    q = select(q, model, experiment, variable, mip, 
        ensemble, version, latest, debug, cmip5)
    for r, p in q.limit(50).all():
        print(os.path.join(p, r))


    



