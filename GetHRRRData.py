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
import GetHRRRFile
from joblib import Parallel, delayed

# HRRR File Naming convention
# CC = model cycle run time (HRRR is run hourly, 00, 06, 12, and 18 out to 24 hours)
# FF = forecast hour (00, 01, etc.)
# 
# hrrr.tCCz.wrfprsfFF.grib2  = 3D pressure level data
# hrrr.tCCz.wrfnatfFF.grib2  = Native level data
# hrrr.tCCz.wrfsfcfFF.grib2  = 2D surface level data
# hrrr.tCCz.wrfsubhfFF.grib2 = 2D surface level sub-hourly data

HRRR_URL_base = 'http://www.ftp.ncep.noaa.gov/data/nccf/com/hrrr/prod'

WriteDirBase = '/Users/lejoflores/MetClimDataScrapers/test_dl'

fcst_cycle = int(sys.argv[1])
year       = int(sys.argv[2])
month      = int(sys.argv[3]) 
day        = int(sys.argv[4])
nProcs     = int(sys.argv[5])

# Error traps
if((fcst_cycle!=0) & (fcst_cycle!=6) & (fcst_cycle!=12) & (fcst_cycle!=18)):
    sys.exit('fcst_cycle must be 0, 6, 12, or 18')

# Construct the name of the url where the GEFS files are
HRRR_URL = HRRR_URL_base+'/hrrr.'+str(year)+f'{month:02}'+f'{day:02}'+'/conus/'

# Open the URL
HRRR_HTMP = urllib.request.urlopen(HRRR_URL)

# Store output in a BeautifulSoup class
HRRR_Soup = bs4.BeautifulSoup(HRRR_HTMP,'lxml')

# Get all the links stored in the BeautifulSoup class and store them
HRRR_Links = HRRR_Soup.findAll('a')

WriteDir         = WriteDirBase+'/'+'hrrr_'+str(year)+f'{month:02}'+f'{day:02}'
WriteDirFcstCyc  = WriteDir+'/f'+f'{fcst_cycle:02}'

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


HRRR_Files = []
HRRR_PathID = []
for Link in HRRR_Links:
    FileLink = Link.get('href') # Get the link of the URL
    if((FileLink.startswith('hrrr.t'+f'{fcst_cycle:02}'+'z')) & ((FileLink.endswith('.grib2')) | (FileLink.endswith('.grib2.idx')))):
        HRRR_Files.append(FileLink)
        

print('Directory structure created. Proceeding to file download...')

Parallel(n_jobs=nProcs, verbose=60, backend='threading')(delayed(GetHRRRFile.GetSingleHRRRFile)(HRRR_URL,HRRR_Files[i],WriteDirFcstCyc) \
         for i in np.arange(len(HRRR_Files)))
