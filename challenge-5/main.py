from database.mysql import MySQLDatabase
from settings import db_config

"""
Retrieve the settings from the
'db_config' dictionary to connect to 
our database so we can instantiate our
MySQLDatabase object
"""
db = MySQLDatabase(db_config.get('db_name'),
                   db_config.get('user'),
                   db_config.get('pass'),
                   db_config.get('host'))

# Get all te available tables for
# our database and print them out.
tables = db.get_available_tables()
print tables

columns = db.get_columns_for_table('articles')
print columns

# Get all the records from
# the people table
all_records = db.select('people')
print "All records: %s" % str(all_records)

# Get all of teh records from
# the people table but only the
# `id` and `first_name` columns
column_specific_records = db.select('people', ['id', 'first_name'])
print "Column specific records: %s" % str(column_specific_records)

# Select data using the WHERE clause
where_expression_records = db.select('people', ['first_name'], 
                                                where="first_name='John'")
print "Where Records: %s" % str(where_expression_records)

# Select data using the WHERE clause and
# the JOIN clause
joined_records = db.select('people', ['first_name'],
                                        where="people.id=3",
                                        join="orders ON people.id=orders.person_id")
print "Joined records: %s" % str(joined_records)

# Select using the limit clause
limited_results = db.select('orders', limit='5')
print "-------------------------------------"
print "Fisrt 5 Results"
print "-------------------------------------"
# iterate over the list of results
for result in limited_results:
    print result
print "-------------------------------------"

# Limit the results to 10
limited_results = db.select('orders', limit='10')
print "First 10 results"
print "-------------------------------------"
for result in limited_results:
    print result

descending_results = db.select('orders', order_desc='amount')
print "---------------------------------------"
print "Descending Results -"
print "---------------------------------------"
for result in descending_results:
    print result

ascending_results = db.select('orders', order_asc='amount')
print "---------------------------------------"
print "Ascending Results -"
print "---------------------------------------"
for result in ascending_results:
    print result