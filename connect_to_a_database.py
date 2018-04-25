import MySQLdb as _mysql

# Connection details for 'my_db' database
db = _mysql.connect(
    db='<db_name>',
    host='localhost',
    user='<db_username>',
    passwd='<db_password>')

# Initialize a cursor object to open
# the database connection
cursor = db.cursor()

# execute the SQL query
cursor.execute("SELECT * FROM people")

# Fetch all the results from the executed query
results = cursor.fetchall()

# close
cursor.close()