import mysql.connector
import pandas as pd

# Connect to MySQL
conn = mysql.connector.connect(
    user='root',
    password='12345',
    host='host.docker.internal', # localhost of host and not the container 
    port='3306',
    database='mock_data'
)

# Create cursor
cursor = conn.cursor()

# Create Places table query
create_table_query = """
CREATE TABLE IF NOT EXISTS Places (
    PlaceID INT AUTO_INCREMENT PRIMARY KEY,
    City VARCHAR(255),
    County VARCHAR(255),
    Country VARCHAR(255)
)
"""

# Execute the query
cursor.execute(create_table_query)

print("Places table created successfully.")

# Read CSV file into a DataFrame
df = pd.read_csv('places.csv')

# Iterate over rows and insert into Places table
for index, row in df.iterrows():
    city = row['city']
    county = row['county']
    country = row['country']
    
    # Insert query
    insert_query = """
    INSERT INTO Places (City, County, Country)
    VALUES (%s, %s, %s)
    """
    
    # Execute the query
    cursor.execute(insert_query, (city, county, country))

# Commit changes
conn.commit()

# Create People table query
create_people_table_query = """
CREATE TABLE IF NOT EXISTS People (
    PersonID INT AUTO_INCREMENT PRIMARY KEY,
    GivenName VARCHAR(255),
    FamilyName VARCHAR(255),
    DateOfBirth DATE,
    PlaceOfBirth VARCHAR(255),
    PlaceID INT,
    FOREIGN KEY (PlaceID) REFERENCES places(PlaceID)
)
"""

# Execute the query to create the People table
cursor.execute(create_people_table_query)

# Read People CSV file into a DataFrame
people_df = pd.read_csv('people.csv')

# Iterate over each row in the People DataFrame
for index, row in people_df.iterrows():
    given_name = row['given_name']
    family_name = row['family_name']
    date_of_birth = row['date_of_birth']
    place_of_birth_city = row['place_of_birth']
    #place_of_birth_county = row['place_of_birth_county']
    #place_of_birth_country = row['place_of_birth_country']
    
    # Query to find PlaceID associated with the place of birth
    query = f"""
    SELECT PlaceID FROM Places 
    WHERE City = '{place_of_birth_city}'
    """
    
    # Execute the query
    cursor.execute(query)
    
    # Fetch the result
    result = cursor.fetchone()
    
    # Check if result is not None
    if result:
        place_id = result[0]
        
        # Insert query
        insert_query = """
        INSERT INTO People (GivenName, FamilyName, DateOfBirth, PlaceOfBirth, PlaceID) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Execute the insert query
        cursor.execute(insert_query, (given_name, family_name, date_of_birth, f'{place_of_birth_city}', place_id))
        
        # Commit changes after each insertion
        conn.commit()
        
        print(f"Data for {given_name} {family_name} inserted successfully.")
    else:
        print(f"Place of birth ({place_of_birth_city}) not found in Places table.")

cursor.execute("CREATE INDEX idx_date_of_birth ON people (DateOfBirth);")


# Close cursor and connection
cursor.close()
conn.close()
