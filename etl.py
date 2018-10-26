#!/usr/bin/env python
# Assumptions #
#   Files: 
#     - csv files are located relative to this file: /fklubdw/FKlubSourceData/*.csv
#   Database: 
#     - Name is 'dis'
#     - No user/pass
#     - Has the necessary schemas. (not necessary if the database cleanup section is finished)
#       - Run 'psql dis < cleanAndCreate.sql' in the terminal while postgres is runnning.
#   Python extra dependencies:
#     - Run in terminal: 'pip3 install pygrametl psycopg2 holidays'
#

import pygrametl
from pygrametl.datasources import CSVSource, MergeJoiningSource, TypedCSVSource
from pygrametl.tables import CachedDimension, SlowlyChangingDimension, BulkFactTable
import psycopg2

import datetime
import holidays
import sys
import time
import csv
import re

### Connection ###
pgconn = psycopg2.connect(database="dis", port=5432, user="postgres", host="localhost")
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')



### Database Cleanup ###
def executeScriptsFromFile(filename):
    # WARNING: Does not work as intended.. From Stackoverflow.

    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            connection.execute(command)
        except Exception as msg:
            print("Command skipped: ", msg)


### Files and Sources ###

csv.register_dialect('fklubDialect', delimiter=';', quoting=csv.QUOTE_NONE)

member_file_handle = open('fklubdw/FKlubSourceData/member.csv')
member_source = TypedCSVSource(member_file_handle,
                               {'id': int, 'active': str, 'year': int,
                                'gender': str, 'want_spam': bool,
                                'balance': int, 'undo_count': int},
                               dialect='fklubDialect')

oldprice_file_handle = open('fklubdw/FKlubSourceData/oldprice.csv')
oldprice_source = TypedCSVSource(oldprice_file_handle,
                                 {'id': int, 'product_id': int,
                                  'price': int, 'changed_on': datetime.datetime},
                                 dialect='fklubDialect')

payment_file_handle = open('fklubdw/FKlubSourceData/payment.csv')
payment_source = TypedCSVSource(payment_file_handle,
                                {'id': int, 'member_id': int,
                                 'timestamp': datetime.datetime, 'amount': int}, dialect='fklubDialect')

product_file_handle = open('fklubdw/FKlubSourceData/product.csv')
product_source = CSVSource(product_file_handle, dialect='fklubDialect')

store_file_handle = open('fklubdw/FKlubSourceData/room.csv')
store_source = TypedCSVSource(store_file_handle,
                              {'id': int, 'name': str, 'description': str},
                              dialect='fklubDialect')

sale_file_handle = open('fklubdw/FKlubSourceData/sale.csv')
sale_source = CSVSource(sale_file_handle, dialect='fklubDialect')


# Time lazy loading. Needs to before fct table declaration

dk_holidays = holidays.DK()

'''
def time_rowexpander(row, namemapping):
    timestamp = pygrametl.getvalue(row, 'timestamp', namemapping)
    (year, month, day, hour, _, _, weekday, _, _) = \
        time.strptime(timestamp, "%Y-%m-%d")

    row['t_year'] = year
    row['t_month'] = month
    row['t_day'] = day
    row['t_hour'] = hour
    row['day_of_week'] = weekday
    row['is_fall_semester'] = month < 2 or month >= 6
    row['is_holiday'] = datetime.date(year, month, day) in dk_holidays
    row['t_timestamp'] = datetime.date(year, month, day)
    return row
'''
def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
    # Here we use driver-specific code to get fast bulk loading.
    # You can change this method if you use another driver or you can
    # use the FactTable or BatchFactTable classes (which don't require
    # use of driver-specifc code) instead of the BulkFactTable class.
    global connection
    curs = connection.cursor()
    curs.copy_from(file=filehandle, table=name, sep=fieldsep,
                   null=str(nullval), columns=atts)

def timestampToTimerow(timestamp):
    row = dict()
    (year, month, day, hour, _, _, weekday, _, _) = \
        time.strptime(timestamp[:19], "%Y-%m-%d %H:%M:%S")

    row['t_year'] = year
    row['t_month'] = month
    row['t_day'] = day
    row['t_hour'] = hour
    row['day_of_week'] = weekday
    row['is_fall_semester'] = month < 2 or month >= 6
    row['is_holiday'] = datetime.date(year, month, day) in dk_holidays
    row['t_timestamp'] = timestamp
    return row


### Dimensions and Facts ###

product_dimension = SlowlyChangingDimension(
    name="dim.product",
    key="product_id",
    attributes=["name", "price", "alcohol_content_ml",
                "activate_date", "deactivate_date", "version",
                "valid_from", "valid_to"],
    lookupatts=["name"],
    versionatt="version",
    fromatt="valid_from",
    toatt="valid_to",
    srcdateatt="lastmoddate",
    cachesize=-1
)


time_dimension = CachedDimension(
    name='dim.time',
    key='time_id',
    attributes=['t_year', 't_month', 't_day', 't_hour',
                'day_of_week', 'is_fall_semester', 'is_holiday', 't_timestamp'],
    lookupatts=["t_year", "t_month", "t_day", "t_hour"]
    #rowexpander=time_rowexpander
)

store_dimension = SlowlyChangingDimension(
    name="dim.store",
    key="store_id",
    attributes=["name", "description",
                "version", "valid_from", "valid_to"],
    lookupatts=["name"],
    versionatt="version",
    fromatt="valid_from",
    toatt="valid_to",
    srcdateatt="lastmoddate",
    cachesize=-1
)

member_dimension = SlowlyChangingDimension(
    name="dim.member",
    key="member_id",
    attributes=["gender", "is_active", "start_year",
                "version", "valid_from", "valid_to"],
    lookupatts=["member_id"],
    versionatt="version",
    fromatt="valid_from",
    toatt="valid_to",
    srcdateatt="lastmoddate",
    cachesize=-1
)

# TODO: Use BulkFactTable or BatchFactTable
fact_table = BulkFactTable(
    name="fct.sale",
    keyrefs=["fk_product_id", "fk_time_id", "fk_store_id", "fk_member_id"],
    measures=["price"],
    bulkloader=pgcopybulkloader
)

### Dimension Filling ###
def fill_product_dimension():
    #id;name;price;active;deactivate_date;quantity;alcohol_content_ml;start_date
    for srcrow in product_source:
        try:
            dimrow = {'product_id': srcrow['id']
                    , 'name': re.sub('<[^<]+?>', '', srcrow['name']).strip() # Remove HTML tags
                    , 'price': srcrow['price']
                    , 'alcohol_content_ml': (srcrow['alcohol_content_ml'] if srcrow['alcohol_content_ml'] != "" else 0.0)
                    , 'activate_date': (srcrow['start_date'] if srcrow['start_date'] != "" else datetime.date(1970, 1, 1))
                    , 'deactivate_date': (srcrow['deactivate_date'] if srcrow['start_date'] != "" else None)
                    , 'version': 1
                    , 'valid_from': datetime.date(1970, 1, 1)
                    , 'valid_to': None}
            product_dimension.insert(dimrow)
        except:
            print(srcrow)
            print(dimrow)
            raise

def fill_member_dimension():
  #id;active;year;gender;want_spam;balance;undo_count
    for srcrow in member_source:
        dimrow = {'member_id': srcrow['id']
                 , 'gender': srcrow['gender']
                 , 'is_active': srcrow['active'] == "t"
                 , 'start_year' : (srcrow['year'] if srcrow['year'] not in ("", 0) else None)
                 , 'version': 1
                 , 'valid_from': datetime.date(1970, 1, 1)
                 , 'valid_to': None}
        
        member_dimension.insert(dimrow)

def fill_store_dimension():
    for srcrow in store_source:
        # in: id, name, description
        dimrow = {'id': srcrow['id'], 'name': srcrow['name'],
                  'description': srcrow['description'],
                  'version': 1, 'valid_from': datetime.date(1970, 1, 1),
                  'valid_to': None}

        # out: name, description, version, valid_from, valid_to=null
        store_dimension.insert(dimrow)

### Fact Filling ###
def fill_fact_table():
  for row in sale_source:
      #productId = 
    # in: id, memberid, product_id, room_id, timestamp, price
    try:
      timerow = timestampToTimerow(row['timestamp'])
      fctrow = { 'fk_product_id': row['product_id']
                , 'fk_time_id': time_dimension.ensure(timerow)
                , 'fk_store_id': row['room_id'] 
                , 'fk_member_id': row['member_id']
                , 'price': row['price']}
      #print(row)
      fact_table.insert(fctrow)
    except:
      print(row)
      raise

### Main ###

def main():
    # TODO: automatic cleanup of the db
    #executeScriptsFromFile('cleanAndCreate.sql')

    # Dimension filling
    
    print("Product")
    fill_product_dimension()
    print("Member")
    fill_member_dimension()
    print("Store")
    fill_store_dimension()    

    # Fact filling
    print("Fact")
    fill_fact_table()

    connection.commit()
    connection.close()


if __name__ == '__main__':
    main()
