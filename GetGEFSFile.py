#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 23:36:21 2018

@author: lejoflores
"""

import urllib.request 

def GetSingleGEFSFile(GEFS_URL,FileLink,PathToWrite):
    
    DownloadURL = GEFS_URL+FileLink
    
    try:
        urllib.request.urlretrieve(DownloadURL,PathToWrite+'/'+FileLink)
    except urllib.request.URLError as e:
        print(e)
      
    print('Successfully downloaded '+FileLink)
    
    return