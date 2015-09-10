#!/usr/bin/env python

import sys
import os # OS stuff
import glob # pathname stuff
import csv # CSV
import json
import argparse # Parse inputs
import re # Regex
from pprint import pprint # Pretty Print

import numpy as np
import shutil

def main( args ):
    # arg 1 = list of csvs
    # arg 2 = list of app

    files = [ line.strip() for line in open('list.txt') ]

    for csv in files:
        tg=csv.split('-')[1]
        n_bg=csv.split('-')[2]
        bg=csv.split('-')[3].split('.')[0]
        shutil.copy(csv, '/home/quan/project/bubble-GPU/accurancy/'+tg+'/'+bg+'/'+csv)

    return 0

if __name__=='__main__':
    sys.exit(main(sys.argv))
