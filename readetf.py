# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 23:23:31 2021

This program will read the etf screen scraps. The screen is etf.

@author: JJ
"""

import datetime as dt
from pandas import DataFrame
import fileread_utils as etf
# initialize record count
reccount = 0

#Initializw file info
filedate = '2021-03-20'
screen = 'etf'
conn = etf.connect_server()
conn.execute("truncate etf_screener")
# convert date string to datetime
fd = dt.date(int(filedate[0:4]) , int(filedate[5:7]) , int(filedate[8:10]))
# retrieve version for target date
vers = etf.get_version(conn ,screen , filedate)
version = vers.first()[0]
# get exclude keywords for current version
kws = etf.get_keywords(conn , version)
# split exclusion keywords into beginning and middle lists
exclude_beg = [ x[0] for x in kws if x[1]=='B']
exclude_mid = [ x[0] for x in kws if x[1]=='M']
# get field info for current version
fields = etf.get_fields(conn , version)

# Field parameters:
#   0 => key
#   1 => version
#   2 => field name
#   3 => position
#   4 => convert func
#   5 => error value
#   6 =? missingsymbol
#   7 => missing value
#   8 => removestring

# Need to create unique list of converrt functions from fields
funclist = list(set([x[4] for x in fields]))

# create dict with function references
funcs = {'rundate' : fd}
for f in funclist:
    funcs[f] = getattr(etf , f)
# initialize list to hold data
data = []

# Process file
fn = "d:\\fin\\TA-RES\\etf%s.txt" % filedate.replace("-" , "")
#open fiile - note encoding
fo = open(fn , "r", encoding = "Latin-1")
# read lines into list
lines = fo.readlines()
# Process each line
for line in lines:
    reccount = reccount + 1
    if len(line) > 5:
        if etf.removerec(line, exclude_beg, exclude_mid) is False:
            cells = line.split('\t')
# create dict to hold all of the fields for the line of data
            items = {'rundate' : fd }
# loop over all fields and add to dict for the row
            for fld in fields:
                items[fld[2]] = funcs[fld[4]](cells[fld[3]] , fld)
# add dict to list of all parsed lines
            data.append(items)
# Put data into dataframe
print("Converting to dataframe")
df = DataFrame (data)
print("Inserting into sql table")
df.to_sql('etf_screener', conn, if_exists='append', index = False)

conn.close()
