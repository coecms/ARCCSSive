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
import iris
from datetime import timedelta
import re
import netcdftime

filecode = {
    'd': ('Dump', {
        'a': 'Instantaneous forecast dump',
        'z': 'Instantaneous before T=zero in assimilation runs',
        }),
    's': ('Partial sum', {
        '1': 'Period 1',
        '2': 'Period 2',
        '3': 'Period 3',
        '4': 'Period 4',
        }),
    'p': ('Output stream', {
        'a': 'Fixed stream a',
        'b': 'Fixed stream b',
        'c': 'Fixed stream c',
        'd': 'Fixed stream d',
        'e': 'Fixed stream e',
        'f': 'Fixed stream f',
        'g': 'Fixed stream g',
        'h': 'Fixed stream h',
        'i': 'Fixed stream i',
        'j': 'Fixed stream j',
        'p': '5-day mean',
        'w': '7-day mean',
        't': '10-day mean',
        'r': '14-day mean',
        'm': 'Monthly mean',
        's': 'Seasonal mean',
        'y': '1-year mean',
        'v': '5-year mean',
        'x': '10-year mean',
        'l': '50-year mean',
        'u': '100-year mean',
        'z': '1000-year mean',
        '1': 'Period 1 mean',
        '2': 'Period 2 mean',
        '3': 'Period 3 mean',
        '4': 'Period 4 mean',
        }),
    }

def decode_time_relative(t):
    """
    Number of hours since model basis time

    >>> decode_time_relative('123')
    datetime.timedelta(5, 10800)
    """
    hours = int(t)
    return timedelta(hours = hours)

def decode_time_sub_hourly(t):
    """
    Number of hours and minutes since model basis time

    >>> decode_time_sub_hourly('a010')
    datetime.timedelta(4, 15000)
    """
    hours = int(t[0], base=36)*10 + int(t[1])
    minutes = int(t[2:4])
    return timedelta(hours = hours, minutes = minutes)

_abstime_re = re.compile('^(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(_(?P<hour>\d{2}))?$')

def decode_time_absolute(t, cal):
    """
    Absolute time
    
    >>> decode_time_absolute('19800105', netcdftime.Datetime360Day)
    netcdftime._netcdftime.Datetime360Day(1980, 1, 5, 0, 0, 0, 0, -1, 1)

    >>> decode_time_absolute('18600910_03', netcdftime.DatetimeProlepticGregorian)
    netcdftime._netcdftime.DatetimeProlepticGregorian(1860, 9, 10, 3, 0, 0, 0, -1, 1)
    """
    match = _abstime_re.match(t)
    if match is None:
        raise ArgumentError

    if match.group('hour') is not None:
        hour = int(match.group('hour'))
    else:
        hour = 0

    date = cal(
            year=int(match.group('year')), 
            month=int(match.group('month')), 
            day=int(match.group('day')),
            hour=hour)

    return date

class RunMeta(object):
    """
    Holds MetaData associated with a UM run
    """
    def __init__(runid):
        self.runid = runid

def decode_um_name(name):
    """
    Returns metadata based on the job name
    """
    jobid = name[0:5]
    model = name[5:6]
    encoding = name[6:7]
    code = name[7:9]
    timecode = name[9:]

def convert_file(runmeta, path, archive):
    basename = os.path.basename(path)

    outfile = os.path.join(archivedir, runmeta.runid, decode_name(basename) + '.nc')

    cubes = iris.load(path)
    iris.save(cubes, outfile, zlib=True)

def main(runid, paths, archive):
    """
    Post-process a UM job

     * Convert UM outputs to NetCDF
     * Add metadata
     * Copy to archive directory
    """

