# -*- coding: utf-8 -*-
"""
   fileread_utils - This module provides untility functions to import data
                    from screenshots from the TDA screener facility. These
                    functions are table driven. The fields and parameters are
                    all stored in multiple SQL tables(currently MariaDB). 
                    Each report of data has a screen value assigned to it for 
                    all versions of the screener. There is a version value for
                    each version of the screener.For each version there are a 
                    set of approriate fields to be input. For each field, there
                    are parameters such as field position, conversion function,
                    missing value information, ... 
    Functions:
           convertpct(xstr : str, cells : list) - converts a string with a % 
                                                  to a float
           get_fields(version : str) - returns a list of tuples containing the
                                        information for each field in the
                                        version.
           get_keywords(versionx : str) - returns a list of tuples containg the
                                          strings for records that should be
                                          ignored.
           get_version(screen : str, readdate : str) - finds the version in  
                                                       effect for a screen on
                                                       any given date.
           def removerec(xstr : str, begs : list, mids : list) - uses the 
                                                exclude lists to determine if 
                                                a record should be ignored. 
    Future enhancements : Convert functions are very basic for now. I need to
                          add error and missing value flexibility. I also need
                          a new convert function to do indexed values from 
                          lookup table. Also need to add error handling.
                

Created on Sat Apr  3 22:43:37 2021
\@author: JJ
"""


#etl_fieldparameters version
#etl_fileformats screen/version/start/end

import mysql.connector
from mysql.connector import errorcode

def convertfloat(xstr : str, cells : tuple ) -> float:
    ''' convertpct converts a string(xstr) into a float. The string is 
        expected to contain a %, but is not necessary. cells contains other
        information such as the value for a missing value, what value to 
        return if value us missing and other characters to remove.'''

# if string is missing value symbol return missing value        
    if xstr == cells[6]:
       return float(cells[7])
# remove any specified character
    xstr = xstr.replace(cells[8],'')
# assign float conversion if no error
    try:
        temp = float(xstr)
# otherwise assign float conversion of error value
    except:
        temp = float(cells[5])
# return value
    else:
        return temp

def convertmorningstarcat(xstr : str, cells : tuple ) -> str:
    ''' convertpct converts a string(xstr) into a float. The string is 
        expected to contain a %, but is not necessary. cells contains other
        information such as the value for a missing value, what value to 
        return if value us missing and other characters to remove. 
        
        Curretly same as convert_str'''
    ''' convertpct converts a string(xstr) into a float. The string is 
        expected to contain a %, but is not necessary. cells contains other
        information such as the value for a missing value, what value to 
        return if value us missing and other characters to remove.'''

# if string equals missing string then return missing value
    if xstr == cells[6]:
        return cells[7]
# otherwise return string unchanged
    else:
        return xstr

def convertthestreet(xstr : str, cells : tuple ) -> int:
    ''' convertpct converts a string(xstr) into a float. The string is 
        expected to contain a %, but is not necessary. cells contains other
        information such as the value for a missing value, what value to 
        return if value is missing and other characters to remove. The street 
        field contains 3 values: Buy/Hold/Sell and missing symbol ---  This 
        function converts these values to integers 1/0,-1 and -99'''
        
# if missing symbol return missing converted missing valuevalue
    if xstr == cells[6]:
        return int(cells[7])
    elif xstr == 'Buy':
        return 1
    elif xstr == 'Sell':
        return -1
    elif xstr == 'Hold':
        return 0
# if string is not ---,Buy, old or Sell return converted error value
    else:
        return int(cells[5])

def convertstring(xstr : str, cells : tuple  ) -> str:
    ''' convertpct converts a string(xstr) into a float. The string is 
        expected to contain a %, but is not necessary. cells contains other
        information such as the value for a missing value, what value to 
        return if value us missing and other characters to remove.'''
        
# if missing string return missing value
    if xstr == cells[6]:
        return cells[7]
# otherwise return string
    else:
        return xstr

def get_fields(version : str) -> list :
    ''' get_fields returns a list of tuples, one for each field to read in the
        input string.'''
    try:
        cnx = mysql.connector.connect(user='root', password='tamelina',
                              host='127.0.0.1',
                              database='fin')
    
    except mysql.connector.Error as err:
       if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
           print("Something is wrong with your user name or password")
       elif err.errno == errorcode.ER_BAD_DB_ERROR:
           print("Database does not exist")
       else:
           print("U:",err)
    else:
        if cnx.is_connected():
            print('Connected to MySQL database')
            cursor = cnx.cursor()
            cursor.execute("select * from etl_fieldparams where version=%s" ,
                           (version , ) )
            res = cursor.fetchall()
    cursor.close()
    cnx.close()
    return res


def get_keywords(versionx : str) -> list :
    ''' This function returns the list keywords used to exclude junk records''' 

    try:
        cnx = mysql.connector.connect(user='root', password='tamelina',
                              host='127.0.0.1',
                              database='fin')
    
    except mysql.connector.Error as err:
       if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
           print("Something is wrong with your user name or password")
       elif err.errno == errorcode.ER_BAD_DB_ERROR:
           print("Database does not exist")
       else:
           print("U:",err)
    else:
        if cnx.is_connected():
            print('Connected to MySQL database')
            cursor = cnx.cursor()
            cursor.execute("select keyword,filtertype from etl_filterwords where etl_version=%s" ,
                           (versionx,) )
            res = cursor.fetchall()
    cursor.close()
    cnx.close()
    return res

def get_version(screen : str, readdate : str) -> tuple :
    ''' get_version returns the version of the screen for a given date. The 
        date should be a string in the form of yyyy-mm-dd'''

    try:
        cnx = mysql.connector.connect(user='root', password='tamelina',
                              host='127.0.0.1',
                              database='fin')
    
    except mysql.connector.Error as err:
       if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
           print("Something is wrong with your user name or password")
       elif err.errno == errorcode.ER_BAD_DB_ERROR:
           print("Database does not exist")
       else:
           print("U:",err)
    else:
        if cnx.is_connected():
            print('Connected to MySQL database')
            cursor = cnx.cursor()
            cursor.execute("select version from etl_fileformats where screen=%s and %s between start and end" ,
                           (screen , readdate ) )
            res = cursor.fetchall()
    cursor.close()
    cnx.close()
    return res

def removerec(xstr : str, begs : list, mids : list) -> bool: 
    ''' remove_rec uses the list of beginning strings and middle strings and
        returns True if the record contans any of them.'''
    tempstr = xstr.strip()
# Test for a string somewhere in the record
    for x in mids:
        if tempstr.find(x) >= 0:
            return True
# Test for a string at the beginning of the record
    for x in begs:
        if tempstr.startswith(x):
            return True
    return False

