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
        tg=csv.split('/')[1].split('-')[1]
        n_bg=csv.split('/')[1].split('-')[2]
        bg=csv.split('/')[1].split('-')[3].split('.')[0]

        if ( (int)(n_bg) == 8 ):
            shutil.copy(csv, '/home/quan/project/bubble-GPU/open-loop/high-load/figs/'+tg+'/'+bg+'/'+csv.split('/')[1])
        elif ((int)(n_bg) == 6):
            shutil.copy(csv, '/home/quan/project/bubble-GPU/open-loop/med-load/figs/'+tg+'/'+bg+'/'+csv.split('/')[1])
        elif ((int)(n_bg) == 4):
            shutil.copy(csv, '/home/quan/project/bubble-GPU/open-loop/low-load/figs/'+tg+'/'+bg+'/'+csv.split('/')[1])
        else:
            shutil.copy(csv, '/home/quan/project/bubble-GPU/open-loop/figs/'+tg+'/'+bg+'/'+csv.split('/')[1])

    return 0

if __name__=='__main__':
    sys.exit(main(sys.argv))
