# Assumptions #
#   Files: 
#     - csv files are located relative to this file: /fklubdw/FKlubSourceData/*.csv
#   Database: 
#     - Name is 'dis'
#     - No user/pass
#     - Has the necessary schemas. (not necessary if the database cleanup section is finished)
#       - Run 'psql dis < cleanAndCreate.sql' in the terminal while postgres is runnning.

import pygrametl
from pygrametl.datasources import CSVSource, MergeJoiningSource, TypedCSVSource
from pygrametl.tables import CachedDimension, SlowlyChangingDimension, FactTable
import psycopg2

import datetime
import sys
import time
import csv

### Connection ###
pgconn = psycopg2.connect(database="dis", port=5432)
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

category_file_handle = open('fklubdw/FKlubSourceData/category.csv')
category_source = TypedCSVSource(category_file_handle,
                                 {'id': int, 'name': str},
                                 dialect='fklubDialect')

member_file_handle = open('fklubdw/FKlubSourceData/member.csv')
member_source = TypedCSVSource(member_file_handle,
                               {'id': int, 'active': bool, 'year': int,
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

product_categories_file_handle = open(
    'fklubdw/FKlubSourceData/product_categories.csv')
product_categories_source = TypedCSVSource(product_categories_file_handle,
                                           {'id': int, 'product_id': int,
                                            'category_id': int},
                                           dialect='fklubDialect')

product_file_handle = open('fklubdw/FKlubSourceData/product.csv')
product_source = TypedCSVSource(product_file_handle,
                                {'id': int, 'name': str, 'price': int,
                                 'active': bool, 'deactivate_date': datetime.datetime,
                                 'quantity': int, 'alcohol_content_ml': float,
                                 'start_date': datetime.datetime},
                                dialect='fklubDialect')

store_file_handle = open('fklubdw/FKlubSourceData/room.csv')
store_source = TypedCSVSource(store_file_handle,
                              {'id': int, 'name': str, 'description': str},
                              dialect='fklubDialect')

sale_file_handle = open('fklubdw/FKlubSourceData/sale.csv')
sale_source = TypedCSVSource(sale_file_handle,
                             {'id': str, 'member_id': int,
                              'product_id': int, 'room_id': int,
                              'timestamp': datetime.datetime, 'price': int},
                             dialect='fklubDialect')


### Dimensions and Facts ###

product_dimension = SlowlyChangingDimension(
    name="dim.product",
    key="product_id",
    attributes=["name", "category", "price", "alcohol_content_ml",
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
    attributes=['t_date', 't_year', 't_month', 't_day', 't_hour',
                'day_of_week', 'is_fall_semester', 'is_holiday'],
    lookupatts=["t_year", "t_month", "t_day", "t_hour"]
    #rowexpander=timehandling TODO
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
    attributes=["gender", "is_active", "course",
                "version", "valid_from", "valid_to"],
    lookupatts=["member_id"],
    versionatt="version",
    fromatt="valid_from",
    toatt="valid_to",
    srcdateatt="lastmoddate",
    cachesize=-1
)

# TODO: Use BulkFactTable or BatchFactTable
fact_table = FactTable(
    name="fct.sale",
    keyrefs=["fk_product_id", "fk_time_id", "fk_store_id", "fk_member_id"],
    measures=["order_id", "item_count"]
)

### Dimension Filling ###

# Store

# TODO
def fill_product_dimension():
    pass

# TODO
# We can fill the time dimension lazily using the rowexpander
# Look at the datehandling function: https://chrthomsen.github.io/pygrametl/

# TODO
def fill_member_dimension():
    pass


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
# TODO
def fill_fact_table():
  pass

### Main ###

def main():
    # TODO: automatic cleanup of the db
    #executeScriptsFromFile('cleanAndCreate.sql')

    # Dimension filling
    fill_product_dimension()
    fill_member_dimension()
    fill_store_dimension()

    # Fact filling
    fill_fact_table()

    connection.commit()
    connection.close()


if __name__ == '__main__':
    main()
