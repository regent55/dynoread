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
           close_connection(conn): -this function closes SQLAlchemny engine

            connect_server(): - creates a zSQLAlchemy engine and connect to db.

           convertpct(xstr : str, cells : list) - converts a string with a %
                                                  to a float
           get_fields(version : str) - returns a list of tuples containing the
                                        information for each field in the
                                        version.
           getpasswd() -> str: - this funcrion retreives the encrypted password
                                 from a binary and decrypts it.
           get_keywords(versionx : str) - returns a list of tuples containg the
                                          strings for records that should be
                                          ignored.
           get_version(screen : str, readdate : str) - finds the version in
                                                       effect for a screen on
                                                       any given date.
           removerec(xstr : str, begs : list, mids : list) - uses the
                                                exclude lists to determine if
                                                a record should be ignored.
           removespec(sstr : str) -> str - This function removes any
                                     non-standard ASCII char from a string.


Created on Sat Apr  3 22:43:37 2021
author: JJ
"""


#import sqlalchemy
from sqlalchemy import create_engine
from cryptography.fernet import Fernet

def getpasswd() -> str:
    ''' Retreive encrypted password '''
    key = b'pRmgMa8T0INjEAfksaq2aafzoZXEuwKI7wDe4c1F8AY='
    cipher_suite = Fernet(key)
    with open('d:\\jj\\passwd.bin', 'rb') as file_object:
        for line in file_object:
            encryptedpwd = line
    uncipher_text = (cipher_suite.decrypt(encryptedpwd))
    return bytes(uncipher_text).decode("utf-8") #convert to string

def close_connection(conn):
    ''' close server connection'''
    conn.close()

def connect_server():
    '''  create connection to server'''
    pw = getpasswd()
    engine = create_engine("mariadb+pymysql://root:"+pw+"@localhost/fin?charset=utf8mb4")
    cnx = engine.connect()
    return cnx

def convertfloat(xstr : str, cells : tuple ) -> float:
    ''' convertfloat converts a string(xstr) into a float. The eighth item of
        the tuple may contain a character like % or $ which will be removed
        from the string before converting to a float. The tuple contains other
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
        return temp
# otherwise assign float conversion of error value
    except:
        temp = float(cells[5])
        return temp

def convertmorningstarcat(xstr : str, cells : tuple ) -> str:
    ''' convertmorningstarcat converts a string(xstr) into a string. Currently
        the function just reguratates the string. In the function it may return
        a foreign key when I separate the sting values into a lookup table.'''
# if string equals missing string then return missing value
    if xstr == cells[6]:
        return cells[7]
# otherwise return string unchanged
    return xstr

def convertthestreet(xstr : str, cells : tuple ) -> int:
    ''' convertthestreet converts a string(xstr) into one of 5 values.
           Buy => 1'
           Hold => 0
           Sell => -1
           Not Defined = -99
           Error => -98'''

# if missing symbol return missing converted missing valuevalue
    if xstr == cells[6]:
        return int(cells[7])
    if xstr == 'Buy':
        return 1
    if xstr == 'Sell':
        return -1
    if xstr == 'Hold':
        return 0
# if string is not ---,Buy, old or Sell return converted error value
    return int(cells[5])

def convertstring(xstr : str, cells : tuple  ) -> str:
    ''' convertstr takes a string and removes non-standard ASCII chars and
        chars specified in thre SQL table.
    '''

# if missing string return missing value
    if xstr == cells[6]:
        return cells[7]
# remove any specified character
    xstr = removespec(xstr)
    xstr = xstr.replace(cells[8] , '')
# At the moment I amhaving trouble reading the resistered trademark symbol
# from HeidiSQL so I am hardcoinguntil I have more time to investigate

# otherwise return string
    return xstr

def get_fields(cnx , version : str) -> list :
    ''' get_fields returns a list of tuples, one for each field to read in the
        input string.'''

    res = cnx.execute("select * from etl_fieldparams where version=%s" ,
                           (version , ) )
    recs = []
    for rec in res:
        recs.append(tuple(rec))
    return recs

def get_keywords(cnx , versionx : str) -> list :
    ''' This function returns the list keywords used to exclude junk records'''

    res = cnx.execute("select keyword,filtertype from etl_filterwords where etl_version=%s" ,
                           (versionx,) )
    recs = []
    for rec in res:
        recs.append(tuple(rec))
    return recs

def get_version(cnx , screen : str, readdate : str) -> tuple :
    ''' get_version returns the version of the screen for a given date. The
        date should be a string in the form of yyyy-mm-dd'''

    res = cnx.execute("select version from etl_fileformats where screen=%s and %s between start and end" ,
                      (screen , readdate))

    return res

def removerec(xstr : str, begs : list, mids : list) -> bool:
    ''' remove_rec uses the list of beginning strings and middle strings and
        returns True if the record contans any of them.'''
    tempstr = xstr.strip()
# Test for a string somewhere in the record
    for kw in mids:
        if tempstr.find(kw) >= 0:
            return True
# Test for a string at the beginning of the record
    for kw in begs:
        if tempstr.startswith(kw):
            return True
    return False

def removespec(sstr : str) -> str:
    ''' removespec builds a string with stanfard ASCII chars only'''
    cleanstr= ""
    for i in sstr:
        num = ord(i)
        if num >=0 :
            if num <= 127:
                cleanstr = cleanstr + i
    return cleanstr
