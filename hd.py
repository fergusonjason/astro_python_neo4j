from typing import Dict
from neo4j_connection import Neo4jConnection
from util import DMS2deg, HMS2deg
import time
import pandas as pd
import configparser

CATALOG_NAME = 'HD'
CATALOG_FULL_NAME = 'Henry Draper Catalogue and Extension 1 (HD,HDE)'
CATALOG_AUTHOR = 'Cannon A.J., Pickering E.C.'

COLUMN_NAMES = ['HD','DM','RAh','RAdm','DE_','DEd','DEm','Ptm','Ptg','SpT','Int','Rem']
COLSPECS = [(0,6),(6,18),(18,20),(20,23),(23,24),(24,26),(26,28),(29,34),(36,41),(42,45),(45,47),(47,48)]

def create_catalog(uri, user, password):

    print("HD: Creating catalog")
    query_string = "MATCH (catalog: CATALOG {name: '$catalog_name'}) WITH COUNT(catalog) > 0 as node_exists RETURN node_exists"
    conn = Neo4jConnection(uri, user, password)
    result = conn.query(query_string, parameters={'catalog_name':'Gliese'})
    conn.close()
    catalog_exists = False
    if result != None and result[0] != None:
        catalog_exists = result[0].get("node_exists")

    if catalog_exists == False:
        query_string = "CREATE (c:CATALOG) " \
            "SET c.name='{}', c.epoch=1900, c.author='{}', c.full_name='{}'".format(CATALOG_NAME, CATALOG_AUTHOR, CATALOG_FULL_NAME)
        conn = Neo4jConnection(uri, user, password)
        result = conn.query(query_string)
        conn.close()

def import_entries(uri: str, user:str, password:str, file_location: str):

    print("HD: importing entries")
    for chunk in pd.read_fwf(file_location, chunksize = 100, colspecs=COLSPECS, names=COLUMN_NAMES):
        for row in chunk.itertuples():
            entry = convert_row_to_dict(row)
            print(entry)

def convert_row_to_dict(row) -> Dict:

    ra = "{} {}".format(row.RAh, row.RAdm)
    ra_dec = HMS2deg(ra, 3)

    dec = "{} {} {}".format(row.DE_, row.DEd, row.DEm)
    dec_dec = DMS2deg(dec, 3)

    result = {
        'number':row.HD,
        'dm': row.DM,
        'ra': ra,
        'ra_dec': ra_dec,
        'dec': dec,
        'dec_dec': dec_dec,
        'photovisual_magnitude': row.Ptm,
        'photographic_magnitude': row.Ptg,
        'spectral_type': row.SpT,
        'intensity': row.Int,
        'remarks': row.Rem

    }

if __name__ == "__main__":

    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    uri = config['Database']['uri']
    user = config['Database']['user']
    password = config['Database']['password']

    create_catalog(uri, user, password)
    import_entries(uri, user, password, 'https://cdsarc.cds.unistra.fr/ftp/III/135A/catalog.dat.gz')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("Gliese: catalog imported in {} seconds".format(total_time))
    pass