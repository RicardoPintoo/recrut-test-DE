from sqlalchemy import Table, Column
import sqlalchemy
import json
import os

# connect to the database
engine = sqlalchemy.create_engine("mysql://root:12345@localhost/mock_data")
connection = engine.connect()

# Assuming `connection` is already established
metadata = sqlalchemy.MetaData()

# Define the table schema
people_table = sqlalchemy.Table('people', metadata, autoload_with=engine)

# Define the path to the JSON file
json_file_path = os.getcwd() + '/data/example_python.json'


# Check if the JSON file exists
#if not os.path.exists(json_file_path):
    #with open(json_file_path, 'w') as json_file:
rows = connection.execute(sqlalchemy.sql.select([people_table]))
print(rows)
        #rows = [{'id': row[0], 'name': row[1]} for row in rows]
        #json.dump(rows, json_file, separators=(',', ':'))