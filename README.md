# dynoread

Hello! Dynoread is my first Python project for public consumption. It is a translatd from SAS code that ais used to read text files. This version reads a text file containing multiple screen scrapes. The process is table driven in the sense that adding or removing fields to read, the informat and outformats used, missing value tokens and values are all accomplished by adding, deleting or editting records in a SQL table. The goal is that one does not have to modify the Python code to adjust what is read or how it is read. For example, on Feburary 22 I added a new field to the screen I was collecteg. To read the new record format all I had to do was add a record to the fields table detailing the field name, what function totranslate its value, what value to use if the functionerred when transating the function, what string constitutes a missing value for the field and what value to return for a missing valur.  

All of the automated features are not yet implemented. The project is somewhat still in the raw phase at the moment as there is very little error handling oncluded.But as it is added it will be table driven wherever possible.

Terms used in this project:
    screen - This is the base screen being scraped. It may have several different layouts over time.
    version - This is a single layout of a screen.
    convert function - Every field has a convert function assigned to it to read the raw values from the screen scrape.
    filedate - The date the date was scraped from the scree,
    exclude list - This is a list of string values which controls whether a line gets processed or ignored.
    
Inputs to the process - A screen value and filedaate.
Output from process - A dataframe with converted data.

Process - The process takes the screen value and filedate to determine the version. Each version has a start and end date. There is no overlap among versions.
          Once the version is determined, it is used to pull the firlds and exclude list for that version from SQL tables. That information is then used to 
          determine whether a line from the file should be ignored or not, read fields and (eventually) handle errors and missing values.
