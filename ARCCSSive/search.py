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
from ARCCSSive.db import connect, Session
from ARCCSSive.CMIP5.Model import *
from ARCCSSive.model.cmip5 import File, Path
import argparse
import sys
from tabulate import tabulate
from sqlalchemy.orm import aliased

keys = ['variable', 'experiment', 'mip', 'model', 'ensemble', 'realm']

def add_filter_args(parser):
    parser.add_argument('--variable', nargs = '*', help="Filter results")
    parser.add_argument('--experiment', nargs='*', help="Filter results")
    parser.add_argument('--mip', nargs='*', help="Filter results")
    parser.add_argument('--model', nargs='*', help="Filter results")
    parser.add_argument('--ensemble', nargs='*', help="Filter results")
    parser.add_argument('--realm', nargs='*', help="Filter results")
    parser.add_argument('--version', nargs='*', help="Filter results")

    parser.add_argument('--all-versions', action='store_true',
            help="Show all available versions")

def parse_args():
    parser = argparse.ArgumentParser(description="Search the CMIP5 catalogue")
    parser.add_argument('--debug', action='store_true',
            help="Show database actions")
    
    subparsers = parser.add_subparsers(help="Commands")

    search_parser = subparsers.add_parser('search', help='Search datasets')
    search_parser.set_defaults(func = search)
    search_parser.add_argument('keys', nargs='*',
            choices=[[]].append(keys),
            help="Columns to list")
    add_filter_args(search_parser)

    file_parser = subparsers.add_parser('files', help='Search files')
    file_parser.set_defaults(func = files)
    add_filter_args(file_parser)

    return parser.parse_args()

def apply_filter(args, q):
    """
    Applies a filter to a query, based on the command line arguments
    """
    #if args.all_versions:
    #    q = q.select_from(Instance).join(Instance.versions)
    #else:
    #    q = q.select_from(Instance).join(Instance.latest_version)

    if args.variable is not None:
        q = q.filter(Instance.variable.in_(args.variable))
    if args.experiment is not None:
        q = q.filter(Instance.experiment.in_(args.experiment))
    if args.mip is not None:
        q = q.filter(Instance.mip.in_(args.mip))
    if args.model is not None:
        q = q.filter(Instance.model.in_(args.model))
    if args.ensemble is not None:
        q = q.filter(Instance.ensemble.in_(args.ensemble))
    if args.realm is not None:
        q = q.filter(Instance.realm.in_(args.realm))
    if args.version is not None:
        q = q.filter(Instance.version.in_(args.version))
    return q

def files(args):
    connect(echo=args.debug)
    session = Session()

    sub = session.query(Path.path).select_from(Instance).join(Instance.latest_version).join(Version.paths)
    sub = apply_filter(args, sub)
    sub = sub.order_by(Path.path)

    for path, in sub:
        print(path)

def search(args):
    connect(echo=args.debug)
    session = Session()

    if len(args.keys) == 0:
        # Show all keys
        args.keys = keys

    columns = []
    if 'variable' in args.keys:
        columns.append(Instance.variable)
    if 'experiment' in args.keys:
        columns.append(Instance.experiment)
    if 'mip' in args.keys:
        columns.append(Instance.mip)
    if 'model' in args.keys:
        columns.append(Instance.model)
    if 'ensemble' in args.keys:
        columns.append(Instance.ensemble)
    if 'realm' in args.keys:
        columns.append(Instance.realm)
    if 'version' in args.keys:
        columns.append(Version.version)

    q = session.query(*columns)
    q = apply_filter(args, q)
    q = q.distinct()

    if sys.stdout.isatty():
        # Pretty print a table of results
        limit = 100
        headers = [x['name'] for x in q.column_descriptions]
        count = q.count()
        print(tabulate(q.limit(limit), headers=headers))
        if count > limit:
            print('---')
            print('Total results: %d'%count)
    else:
        for r in q:
            print("\t".join(r))

def main():
    args = parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
