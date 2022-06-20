import requests
from datetime import date, datetime, timedelta
import sqlalchemy as db
import json

# Database connection
engine = db.create_engine('sqlite:///problems_db.sqlite')
connection = engine.connect()
metadata = db.MetaData()

# Build climate table
if db.inspect(engine).dialect.has_table(connection, 'climate'):
    engine.execute('DROP TABLE climate')

climate_table = db.Table('climate', metadata,
                         db.Column('id', db.Integer, primary_key=True),
                         db.Column('date', db.String),
                         db.Column('datatype', db.String),
                         db.Column('station', db.String),
                         db.Column('attributes', db.String),
                         db.Column('value', db.Integer))

metadata.create_all(engine)

# URL for NOAA datasets
noaa = "https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid="

# NOAA dataset access token
h = {"token": "cBzKIKbjojRJUnLZNjEQtbXVeXZEPkyR"}

# NOAA dataset id
id = "GHCND"

# Start and end date for data pull
startdate = datetime(1970, 1, 1, 0, 0, 0)
enddate = startdate + timedelta(days=1)

# Grab content from fips table
fips = db.Table('fips', metadata, autoload=True, autoload_with=engine)
fips_data = connection.execute(db.select([fips])).fetchall()
print(len(fips_data))

# Insert climate data into climate table
for i in range(0, len(fips_data)):
    climate_data = json.loads(requests.get(noaa + id + "&limit=1000&format=json" + "&locationid=FIPS:"+ fips_data[i][2] + fips_data[i][3] + "&startdate=" + str(startdate.isoformat()) + "&enddate=" + str(enddate.isoformat()), headers=h).content)
    if 'results' in climate_data:
        for r in climate_data['results']:
            ins = db.insert(climate_table).values(date=r['date'], datatype=r['datatype'], station=r['station'], attributes=r['attributes'], value=r['value'])
            print('Inserting {}'.format(r))
            connection.execute(ins)