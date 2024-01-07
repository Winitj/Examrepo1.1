
import mysql.connector
from sqlalchemy import create_engine
import dask.dataframe as dd

# MySQL database connection settings
db_settings = {
    'host': 'localhost',
    'port': '3307',  # Use the IP or hostname of your MySQL container
    'user': 'root',
    'password': 'Wen2003',
    'database': 'vinit',
}

# Your existing code to read and process CSV files using Dask
base_mapping = {
    'B02512': 'Unter',
    'B02598': 'Hinter',
    'B02617': 'Weiter',
    'B02682': 'Schmecken',
    'B02764': 'Danach-NY',
    'B02765': 'Grun',
    'B02835': 'Dreist',
    'B02836': 'Drinnen'
}

file_paths = [
    "C://Users//jain vinit//Documents//FExam//uber-tlc-foil-response//uber-trip-data//uber-raw-data-aug14.csv",
    "C://Users//jain vinit//Documents//FExam//uber-tlc-foil-response//uber-trip-data//uber-raw-data-jul14.csv",

]

ddf_list = [dd.read_csv(file) for file in file_paths]
df = dd.concat(ddf_list)

# Additional processing, mapping, etc.
df['Base'] = df['Base'].map(base_mapping.get, na_action='ignore')

# MySQL connection using SQLAlchemy and connection pooling
engine = create_engine(
    f"mysql+mysqlconnector://{db_settings['user']}:{db_settings['password']}@{db_settings['host']}:{db_settings['port']}/{db_settings['database']}",
    pool_size=10,  # Adjust pool size based on your needs
    max_overflow=20
)

# Create the table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS uber_data (
    Date DATE,
    Lat FLOAT,
    Lon FLOAT,
    Base VARCHAR(255)
);
"""
with engine.connect() as connection:
    connection.execute(create_table_query)

# Load CSV data into MySQL table
df.compute().to_sql('uber_data3', con=engine, if_exists='replace', index=False)
