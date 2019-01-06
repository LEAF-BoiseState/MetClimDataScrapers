#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  5 21:05:27 2019

@author: lejoflores
"""

import urllib.request 

def GetSingleHRRRFile(HRRR_URL,FileLink,PathToWrite):
    
    DownloadURL = HRRR_URL+FileLink
    
    print('Trying to download file '+DownloadURL)
    
    try:
        urllib.request.urlretrieve(DownloadURL,PathToWrite+'/'+FileLink)
    except urllib.request.URLError as e:
        print(e)
        return
      
    print('Successfully downloaded '+FileLink)
    
    return