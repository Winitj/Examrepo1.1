
import dask.dataframe as dd
import pandas as pd

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
    "C://Users//jain vinit//Documents//FExam//uber-tlc-foil-response//uber-trip-data//uber-raw-data-jun14.csv", 
    "C://Users//jain vinit//Documents//FExam//uber-tlc-foil-response//uber-trip-data//uber-raw-data-may14.csv", 
    "C://Users//jain vinit//Documents//FExam//uber-tlc-foil-response//uber-trip-data//uber-raw-data-sep14.csv",
    "C://Users//jain vinit//Documents//FExam//uber-tlc-foil-response//uber-trip-data//uber-raw-data-apr14.csv", 
]

ddf_list = [dd.read_csv(file) for file in file_paths]
df = dd.concat(ddf_list)


df['Base'] = df['Base'].map(base_mapping.get, na_action='ignore')
print(df.tail(5))