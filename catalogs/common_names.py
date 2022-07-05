
import sys, os, inspect
from typing import Dict, Any
from urllib.request import urlretrieve
import pandas as pd
import time

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from util import DMS2deg, HMS2deg
from config.config import URI, USER, PASSWORD
from util import get_greek_letter

COLUMN_NAMES = ['Name','Designation','ID','Constellation','Magnitude','Hipparcos','HD','RA','DEC']
COLSPECS = [(0,18),(36,49),(49,54),(61,64),(82,88),(90,97),(97,104),(104,115),(116,126)]

def do_common():
    start_time = time.time()

    import_entries(URI, USER, PASSWORD, 'https://raw.githubusercontent.com/mirandadam/iau-starnames/master/catalog_data/IAU-CSN.txt')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("HR: catalog imported in {} seconds".format(total_time))

def import_entries(uri: str, user:str, password:str, file_location: str):
    print("Common names: importing entries")

    if not os.path.exists('/tmp/common_catalog.txt'):
        print("Common names: catalog file not found locally, downloading")
        urlretrieve(url=file_location, filename="/tmp/common_catalog.txt")[0]
        print("Common names: Catalog downloaded")
    else:
        print("Common names: found cached hr_catalog.gz")

    for chunk in pd.read_fwf('/tmp/common_catalog.txt', chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES, skiprows=20, nrows=449):

        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))

        print(batch)
        # query_string = "WITH $batch as batch " \
        #     "UNWIND batch as item " \
        #     "CREATE (s:star) SET s+= item " \
        #     "RETURN id(s) as id"
        # conn = Neo4jConnection(uri, user, password)
        # response = conn.query(query_string, parameters={'batch':batch})

        # if response != None and response[0] != None:
        #     id_list = []
        #     for record in response:
        #         id_list.append(record.get('id'))

        #     query_string = "WITH $idlist as id_list " \
        #         "UNWIND id_list as item " \
        #         "MATCH (c:CATALOG), (s:STAR) " \
        #         "WHERE c.name = 'Common Names' and id(s) = item " \
        #         "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
        #         "RETURN ce"
        #     response = conn.query(query_string, parameters={'idlist': id_list})

def convert_row_to_dict(row) -> Dict[str, Any]:

    id = None
    if row.ID != '_':
        id = row.ID

    hd = None
    if row.HD != '_':
        hd = int(row.HD)

    hip = None
    if row.Hipparcos != '_':
        hip = int(row.Hipparcos)

    result = {
        'catalog':'Common Names',
        'name': row.Name,
        'designation': row.Designation,
        'ID': id,
        'constellation':row.Constellation,
        'magnitude': row.Magnitude,
        'HIP number': hip,
        'HD': hd,
        'ra': round(float(row.RA),3),
        'dec': round(float(row.DEC),3)
    }

    return result

if __name__ == "__main__":

    do_common()