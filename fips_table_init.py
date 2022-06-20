import requests
import sqlalchemy as db

# Database connection
engine = db.create_engine('sqlite:///problems_db.sqlite')
connection = engine.connect()
metadata = db.MetaData()

# Build fips table
if db.inspect(engine).dialect.has_table(connection, 'fips'):
    engine.execute('DROP TABLE fips')

fips_table = db.Table('fips', metadata,
                      db.Column('id', db.Integer, primary_key=True),
                      db.Column('state', db.String),
                      db.Column('state_id', db.String),
                      db.Column('county_id', db.String),
                      db.Column('county', db.String))

metadata.create_all(engine)

# URL for county FIPS codes
fips = "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"

# Download fips data
fips_data = requests.get(fips).content.decode('UTF-8').split('\r\n')

# Insert fips data into fips table
for i in range(0, len(fips_data)-1):
    row = fips_data[i].split(',')
    ins = db.insert(fips_table).values(state=row[0], state_id=row[1], county_id=row[2], county=row[3])
    connection.execute(ins)