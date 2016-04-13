#!/usr/bin/env python
# This check data available on ESGF and on raijin that matches constraints passed on by user and return a summary.
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

#from ARCCSSive import CMIP5
from ARCCSSive.CMIP5.pyesgf_functions import *
from ARCCSSive.CMIP5.other_functions import *
import argparse
import os,sys
import commands
from shutil import copyfile

# check python version and then call main()
if sys.version_info < ( 2, 7, 9):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7.9 or newer!")

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description=r''' Downloads all the CMIP5 files
             listed in file/s passed as input argument and calculate checksums.||
             Accepts more than one input file (required) and one optional log file||
             Input file has format:||
             file url, checksum type, checksum ''')
    parser.add_argument('-i','--input', type=str, nargs="*", help='name of input file/s with download urls', required=True)
    parser.add_argument('-id','--openid', type=str, nargs=1, help='user password on ESGF', required=False)
    parser.add_argument('-p','--password', type=str, nargs=1, help='user openid on ESGF', required=False)
    parser.add_argument('-l','--log', type=str, nargs=1, help='urls to download output file name', required=False)
    return vars(parser.parse_args())


def download_file(furl,cks_type,cks,insecure):
    ''' download a file using wget file_url '''
    filename=furl.split("/")[-1]
    wget_args="wget "
    if insecure:
       wget_args+="--no-check-certificate "
    else:
       wget_args+="--ca-directory=$HOME/.esg/certificates "
    wget_args+="--cookies=on --keep-session-cookies --save-cookies $HOME/.esg/wget_cookies/wcookies.txt "
    wget_args+=furl
    print(wget_args)
    #try:
    #  run_status = commands.getstatusoutput(wget_args)
    #  print(run_status)
    #except:
    #  print('Download failed for file: '+filename)
    new_wget1="wget https://pcmdi.llnl.gov/esgf-idp/openid/ --no-check-certificate  -O-"
    new_wget2='''wget --post-data "openid_identifier=https://pcmdi.llnl.gov/esgf-idp/openid/&rememberOpenid=on"
               --header="esgf-idea-agent-type:basic_auth" --http-user="paolap2" --http-password="FM27g201@" 
               --ca-directory=/home/581/pxp581/.esg/certificates --cookies=on --keep-session-cookies --save-cookies 
               /home/581/pxp581/.esg/wget_cookies/wcookies.txt  --load-cookies /home/581/pxp581/.esg/wget_cookies/wcookies.txt  -v -O'''
    new_wget2+= filename + " https://esgf.extra.cea.fr/esg-orp/j_spring_openid_security_check.htm"
    run_status = commands.getstatusoutput(new_wget1)
    run_status = commands.getstatusoutput(new_wget2)
      
    hash = check_hash(fpath+filename,cks_type.lower())
    return filename, hash==cks 

args=parse_input()
insecure=False
flog="log_download_"+today+".txt"
if args['log']: flog=args['log'][0]

# open connection to ESGF
node_url="http://pcmdi.llnl.gov/esg-search"
openid="https://pcmdi.llnl.gov/esgf-idp/openid/paolap2"
password="FM27g201@"
session=logon(openid,password)
if not session.is_logged_on():
   print("User ", openid.split("/")[-1], "could not log onto node ", openid.split("/")[2])
else:
   print("User successfully logged on ESGF")


for fname in args['input']: 
    wgetout="wget_"+fname.replace(".txt",".sh")
    fw=open(wgetout,"w")
    flist=["wget_template1.sh",fname,"wget_template2.sh"]
    for f in flist:
       ftmp=open(f,"r")   
       for line in ftmp.readlines():
          fw.write(line)
       ftmp.close()
    fw.close()
sys.exit()
# move to user temporary directory on ua6
os.chdir("/g/data1/ua6/unofficial-ESG-replica/tmp/" + os.environ["USER"]) 
for line in lines:
    print(line)
    fname,bool=download_file(furl,cks_type,cks,insecure)
    if not bool: log_dict['fname']="checksum failed, esgf "+ cks_type + str(cks)

# close connection to ESGF
if logoff(session):
   print("User successfully logged off ESGF")
else:
   print("User could not log off ESGF")

for k,v in log_dict.items():
    flog.write(k + ":" + v)
flog.close()
