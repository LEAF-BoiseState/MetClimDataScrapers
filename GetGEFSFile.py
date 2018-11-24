#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 23 23:36:21 2018

@author: lejoflores
"""

import urllib.request 

def GetSingleGEFSFile(FileLink,PathToWrite):
    
    try:
        urllib.request.urlretrieve(FileLink,PathToWrite)
    except urllib.request.URLError as e:
        print(e)
        
