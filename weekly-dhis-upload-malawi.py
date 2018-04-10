# -*- coding: utf-8 -*-
"""
Partners in Health DHIS Data Warehousing Project

Objective: Develop a tool that extracts data from DHIS2 Web API and loads into central data warehouse
Country: Malawi

Steps:
    1) Create Malawi MySQL Database
    2) Run this script - set variable to allow total upload
    3) Run this script - set variable to allow weekly upload
    4) Set up Cron Scheduler to run on weekly basis
"""

# Import all relevant packages
import requests
import json
from pprint import pprint
import pandas as pd
import itertools
import re
import mysql.connector
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
import datetime


###########################################################################
# Set Permanent Global variables
###########################################################################

####set variable to allow total upload
## To re-run the initial install and import all metadata from the start,
## un-comment the initialize line.
__name__ = "initialize"

# Source DHIS2 credentials and base URL
authorization = ('TODO-DHIS-USERNAME', 'TODO-DHIS-PASSWORD')
base_url = "https://TODO-SERVER-URL/dhis/api"

# Target database credentials and database name
sql_user = "TODO-SQL-USERNAME"
sql_pw = "TODO-SQL-PASSWORD"
sql_db = "malawi_dhis_warehouse"

# Set Time period
if __name__ == "__main__":
    period = "pe:LAST_12_MONTHS"

else:
    years = ["2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008",
             "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    all_months = ';'.join([year + month for year, month in itertools.product(years, months)])
    period = "pe:" + all_months


############################################################################
# Pull informational data
# E.g., Indicator Groups, Indicators, Organisation Units
############################################################################


################
# Pull Org Units
################
# Create organization units dictionary

#Base OU url
orgUnitsUrl = base_url + "/organisationUnits?paging=False"

#Function that pulls each of the org units by level
def pull_orgs(orgUnitsUrl, level):
    #request and load into json
    orgs = requests.get(orgUnitsUrl + "&" + level, auth = authorization)
    orgs = json.loads(orgs.text)

    #iterate and append id and and displayname
    orgUnitsUrl_list = []
    for o in orgs['organisationUnits']:
        for key, value in o.items():
            orgUnitsUrl_list.append(value)

    #Convert to dictionary to easily pull in and out.  Can just use keys for params
    orgUnits = dict(itertools.zip_longest(*[iter(orgUnitsUrl_list)] * 2, fillvalue=""))

    return orgUnits

# Haiti will need to add an extra level (wards)
orgUnits = pull_orgs(orgUnitsUrl, "level=3")
districts = pull_orgs(orgUnitsUrl, "level=2")
countries = pull_orgs(orgUnitsUrl, "level=1")

#########################
# Map Facility Ancestors
#########################

#Map out the parent relations of each facility
relationsurl = base_url + "/organisationUnits"
relationships = {}

for key in orgUnits.keys():
    #pull out information for key
    relations = requests.get(relationsurl + "/" + key, auth = authorization)
    relations = json.loads(relations.text)

    #only want ID
    ancestors = str(relations['ancestors'])
    ancestors = re.findall(r' \'(.*?)\'}', ancestors)

    #Create dictionary
    relationships[key]=' '.join(ancestors)


##################################
# Pull out Indicator Groups
##################################
# Each indicator is part of a program (Indicator Groups)
# We pull out the indicator groups and use these groups to pull out indicators

# Base URL
indicatorGroupsUrl = base_url + "/indicatorGroups?paging=False"

# Request and load into json
elements = requests.get(indicatorGroupsUrl, auth = authorization)
elements = json.loads(elements.text)


# Iterate and append id and and displayname
indicatorGroups_list = []
for e in elements['indicatorGroups']:
    for key, value in e.items():
        indicatorGroups_list.append(value)

# Convert to dictionary to easily pull in and out.  Can just use keys for params
indicatorGroups = dict(itertools.zip_longest(*[iter(indicatorGroups_list)] * 2, fillvalue=""))


# MySQL tables will be named after Indicator Groups
# Replace names with names amenable to MySQL
for key, value in indicatorGroups.items():
    value = value.replace("-","_")
    value = value.replace(" ", "_")
    value = value.replace("__","_")
    value = re.sub(r"[?|$|\.|!|\(|\)]", "", value)
    indicatorGroups[key] = value


##################################
# Create list of Indicators
##################################
indicatorsUrl = base_url + "/indicators?paging=False"

#request and load into json
elements = requests.get(indicatorsUrl, auth = authorization)
elements = json.loads(elements.text)


#iterate and append id and and displayname
indicators_list = []
for e in elements['indicators']:
    for key, value in e.items():
        indicators_list.append(value)

#Convert to dictionary to easily pull in and out.  Can just use keys for params
indicators = dict(itertools.zip_longest(*[iter(indicators_list)] * 2, fillvalue=""))


###############################
# Prepare request queries
###############################
# Base URL and set base dimensions
dimensions_base = "dx:IN_GROUP-"
base =base_url + "/analytics"

# CAT organisation units
orgs = "ou:"
for unit in orgUnits.keys():
    orgs = orgs + unit + ";"

orgs = orgs[:-1]

############################################################################
# Define Functions to pull relevant data and upload
############################################################################
#Pull all relevant data from API
def pull_data(dimensions_base, indicator_group, base, time_period, orgs):


    dimensions = dimensions_base + indicator_group
    params = dict(
        dimension = [dimensions, time_period, orgs])

    r = requests.get(base, params = params,
               auth = authorization)

    data = json.loads(r.text)

    #Check the columns names from their email and replace with those
    data = pd.DataFrame(data['rows'], columns = ["indicator","date", "facility", "value"])
    data['ID'] = indicator_group

    #map on org units relationships - with org unit dictionaries
    data['temp'] = data['facility']
    data = data.replace({"temp":relationships})
    data["country"], data["district"] = data['temp'].str.split(" ", 1).str
    data.drop('temp', axis =1, inplace = True)

    #Replace codes with english names
    data = (data.replace({"indicator":indicators, "facility":orgUnits,
                          "ID":indicatorGroups,
                          "country":countries, "district":districts}))

    data['date'] = pd.to_datetime(data['date'], format = '%Y%m')

    return data



#function checks if a table exists and if not then create it
def create_table(data, indicator_group):
    #read in table name from indicator groups list
    table_name = indicatorGroups.get(indicator_group).lower()

    #identify whether a table exists
    check_table_query = ("SHOW TABLES LIKE '" + table_name + "'")
    cursor.execute(check_table_query)

    #if table has value then the table exists in the database
    exists = False
    for table in cursor:
        exists = True if len(table) > 0  else False
        print(table_name + " Exists in MySQL Database already")

    #If exists is true then remove the last 12 months of data from table
    if exists == True:
        year_ago = datetime.date.today() - datetime.timedelta(365)
        year_ago = year_ago.replace(day = 1)

        drop_12_months_query = ("delete from " + table_name + " where date >= %s;")
        cursor.execute(drop_12_months_query, (year_ago,))
        con.commit()

    #Otherwise create a new table
    elif exists == False: ##Errors around here may be due to oddly named indicator groups
        create_table_query = ("Create table " + table_name + """ (
                ID varchar(50) not null,
                Country varchar(50) not null,
                District varchar(100) not null,
                Facility varchar(125) not null,
                Indicator varchar(200) not null,
                Date date not null,
                Value numeric(25,2) not null);""")

        cursor.execute(create_table_query)
        con.commit()

    #Upload data to server
    data.to_sql(con = engine, name = table_name, if_exists = "append", index = False)


############################################################################
# Run File
############################################################################
def main(indicator_group):
    data = pull_data(dimensions_base, indicator_group, base, period, orgs)
    create_table(data, indicator_group)

###Establish MySQL connection
con = mysql.connector.connect(user = sql_user, password = sql_pw, database = sql_db)
cursor = con.cursor()
#MySQL is deprecated in current pandas - need to manually create sql engine
engine = create_engine("mysql+pymysql://" +sql_user + ":" + sql_pw + "@localhost/" + sql_db)

#loop through all indicator groups
for key in indicatorGroups:
    main(key)
    pprint(indicatorGroups.get(key).lower())

cursor.close()
con.close()
