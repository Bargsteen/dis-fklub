import pygrametl
from pygrametl.datasources import CSVSource, MergeJoiningSource
from pygrametl.tables import CachedDimension, SlowlyChangingDimension, BulkFactTable
import psycopg2

import datetime
import sys
import time

# Connection
pgconn = psycopg2.connect(database="dis", port=5432)
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')


# Methods
def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
    # Here we use driver-specific code to get fast bulk loading.
    # You can change this method if you use another driver or you can
    # use the FactTable or BatchFactTable classes (which don't require
    # use of driver-specifc code) instead of the BulkFactTable class.
    global connection
    curs = connection.cursor()
    curs.copy_from(file=filehandle, table=name, sep=fieldsep,
                   null=str(nullval), columns=atts)


# Dimensions
productDim = SlowlyChangingDimension(
  name="dim.product",
  key="product_id",
  attributes=["name", "price", "alcohol_content_ml", 
  "activate_date", "deactivate_date", "version", "valid_from", "valid_to"],
  lookupatts="name",
  versionatt="version",
  fromatt="valid_from",
  toatt="valid_to",
  srcdateatt="lastmoddate",
  cachesize=-1
)


timeDim = CachedDimension(
    name='dim.time',
    key='time_id',
    attributes=['t_date', 't_year', 't_month', 't_day', 't_hour', 
    'day_of_week', 'fall_semester', 'is_holiday'],
    lookupatts="t_date"
    )

storeDim = SlowlyChangingDimension(
  name="dim.store",
  key="store_id",
  attributes=["city", "building", "room", "version", "valid_from", "valid_to"],
  lookupatts="building", # Should be something unique
  versionatt="version",
  fromatt="valid_from",
  toatt="valid_to",
  srcdateatt="lastmoddate",
  cachesize=-1
)



