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
import GetGEFSFile
from joblib import Parallel, delayed



GEFS_URL_base = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs'
GEFS_Grid     = 'pgrb2'

nEns = 21

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
GEFS_URL = GEFS_URL_base+'.'+str(year)+str(month)+str(day)+'/'+str(fcst_cycle)+'/'+GEFS_Grid+'/'

# Open the URL
GEFS_HTMP = urllib.request.urlopen(GEFS_URL)

# Store output in a BeautifulSoup class
GEFS_Soup = bs4.BeautifulSoup(GEFS_HTMP,'lxml')

# Get all the links stored in the BeautifulSoup class and store them
GEFS_Links = GEFS_Soup.findAll('a')

WriteDir         = WriteDirBase+'/'+'gefs_'+str(year)+f'{month:02}'+f'{day:02}'
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

WriteDirEns = []
for i in np.arange(nEns):
    WriteDirEns.append(WriteDirFcstCyc+'/'+'ens'+f'{i:02}')
    os.mkdir(WriteDirEns[i])

GEFS_Files = []
GEFS_PathID = []
for Link in GEFS_Links:
    FileLink = Link.get('href') # Get the link of the URL
    if((FileLink.startswith('gep')) | (FileLink.startswith('gec'))):
        GEFS_Files.append(FileLink)
        GEFS_PathID.append(int(FileLink[3:5]))

print('Directory structure created. Proceeding to file download...')

Parallel(n_jobs=nProcs, verbose=60, backend='threading')(delayed(GetGEFSFile.GetSingleGEFSFile)(GEFS_URL,GEFS_Files[i],WriteDirEns[GEFS_PathID[i]]) \
         for i in np.arange(len(GEFS_Files)))
