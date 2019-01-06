#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  5 21:06:32 2019

@author: lejoflores
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 21:28:32 2018

@author: lejoflores
"""

import bs4 # BeautifulSoup4
import sys 
import os
import urllib.request 
import numpy as np
import shutil
import GetCFSv2File
from joblib import Parallel, delayed

# HRRR File Naming convention
# CC = model cycle run time (HRRR is run hourly, 00, 06, 12, and 18 out to 24 hours)
# FF = forecast hour (00, 01, etc.)
# 
# hrrr.tCCz.wrfprsfFF.grib2  = 3D pressure level data
# hrrr.tCCz.wrfnatfFF.grib2  = Native level data
# hrrr.tCCz.wrfsfcfFF.grib2  = 2D surface level data
# hrrr.tCCz.wrfsubhfFF.grib2 = 2D surface level sub-hourly data

CFSv2_URL_base = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/cfs/prod/cfs/'

WriteDirBase = '/Users/lejoflores/MetClimDataScrapers/test_dl'

fcst_cycle = int(sys.argv[1])
year       = int(sys.argv[2])
month      = int(sys.argv[3]) 
day        = int(sys.argv[4])
ens        = int(sys.argv[5])
nProcs     = int(sys.argv[6])

# Error traps
if((fcst_cycle!=0) & (fcst_cycle!=6) & (fcst_cycle!=12) & (fcst_cycle!=18)):
    sys.exit('fcst_cycle must be 0, 6, 12, or 18')

if((ens<1) | (ens>4)):
    sys.exit('ens must be 1, 2, 3 or 4')


# Construct the name of the url where the GEFS files are
CFSv2_URL = CFSv2_URL_base+'/cfs.'+str(year)+f'{month:02}'+f'{day:02}'+'/'+f'{fcst_cycle:02}'+'/'
CFSv2_URL += '6hrly_grib_'+f'{ens:02}'+'/'

# Open the URL
CFSv2_HTMP = urllib.request.urlopen(CFSv2_URL)

# Store output in a BeautifulSoup class
CFSv2_Soup = bs4.BeautifulSoup(CFSv2_HTMP,'lxml')

# Get all the links stored in the BeautifulSoup class and store them
CFSv2_Links = CFSv2_Soup.findAll('a')

WriteDir         = WriteDirBase+'/'+'cfsv2_'+str(year)+f'{month:02}'+f'{day:02}'
WriteDirFcstCyc  = WriteDir+'/f'+f'{fcst_cycle:02}'+'_e'+f'{ens:02}'

while True:
    try:
        os.mkdir(WriteDir)
        break
    except OSError:
        try:
            shutil.rmtree(WriteDir)
            continue
        except OSError:
            sys.exit('Could not create directory '+WriteDir)
            
os.mkdir(WriteDirFcstCyc)


CFSv2_Files = []
CFSv2_PathID = []
for Link in CFSv2_Links:
    FileLink = Link.get('href') # Get the link of the URL
    if(FileLink.endswith('.grb2') | FileLink.endswith('.grb2.idx')):
        CFSv2_Files.append(FileLink)
        

print('Directory structure created. Proceeding to file download...')

Parallel(n_jobs=nProcs, verbose=60, backend='threading')(delayed(GetCFSv2File.GetSingleCFSv2File)(CFSv2_URL,CFSv2_Files[i],WriteDirFcstCyc) \
         for i in np.arange(len(CFSv2_Files)))
