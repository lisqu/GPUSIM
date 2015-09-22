#! /bin/bash

ls simulated/*.csv > list.txt
#sort -n list.txt > csv_list.txt
#rm list.txt

./copy.py
