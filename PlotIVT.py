#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 24 22:50:48 2018

@author: lejoflores
"""

import gdal
import numpy as np
import matplotlib.pyplot as plt

import pandas as pd

grb_file_base = 'test_dl/gefs_20181124/18/ens01/gep01.t18z.pgrb2f'



grb_file = grb_file_base+'00.grb'

grb_ds  = gdal.Open(grb_file,gdal.GA_ReadOnly)


if grb_ds is not None: 
    BandCnt = int(grb_ds.RasterCount)


GEFS_BandID = []
GEFS_BandNames = []
GEFS_Descriptions = []

for i in np.arange(BandCnt):

    band = grb_ds.GetRasterBand(int(i+1))        
    metadata = band.GetMetadata()
    GEFS_BandID.append(i+1)
    GEFS_BandNames.append(metadata['GRIB_COMMENT'])
    GEFS_Descriptions.append(band.GetDescription())

grb_ds = None


df = pd.DataFrame()

df['BandID'] = GEFS_BandID
df['VarName'] = GEFS_BandNames
df['VarDesc'] = GEFS_Descriptions

#for ff in np.arange(0,384+1,6):
#    grb_file = grb_file_base+f'{ff:02}'
#    print(grb_file)
    