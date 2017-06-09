#!/bin/bash
#  Copyright 2017 ARC Centre of Excellence for Climate Systems Science
#
#  \author  Scott Wales <scott.wales@unimelb.edu.au>
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

set -eu

ARCCSSIVE_USER=postgres
ARCCSSIVE_DB=postgresql://localhost/postgres

psql -U $ARCCSSIVE_USER db/000_nci.sql
psql -U $ARCCSSIVE_USER -f db/001_raw.sql
psql -U $ARCCSSIVE_USER -f db/002_model.sql
python ARCCSSive/cftable.py
psql -U $ARCCSSIVE_USER -f db/010_refresh.sql
