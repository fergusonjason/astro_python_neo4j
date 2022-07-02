from typing import Dict
from neo4j_connection import Neo4jConnection
from util import DMS2deg, HMS2deg
import time
import pandas as pd
import configparser
from math import modf, floor
import astropy
from astropy.coordinates import SkyCoord
from astropy import units as u
import re
import numpy as np

CATALOG_NAME = 'HD'
CATALOG_FULL_NAME = 'Henry Draper Catalogue and Extension 1 (HD,HDE)'
CATALOG_AUTHOR = 'Cannon A.J., Pickering E.C.'

COLUMN_NAMES = ['HD','DM','RAh','RAdm','DE_','DEd','DEm','Ptm','Ptg','SpT','Int','Rem']
COLSPECS = [(0,6),(6,18),(18,20),(20,23),(23,24),(24,26),(26,28),(29,34),(36,41),(42,45),(45,47),(47,48)]

def do_hd():
    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    uri = config['Database']['uri']
    user = config['Database']['user']
    password = config['Database']['password']

    import_entries(uri, user, password, 'https://cdsarc.cds.unistra.fr/ftp/III/135A/catalog.dat.gz')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("HD: catalog imported in {} seconds".format(round(total_time, 3)))

def import_entries(uri: str, user:str, password:str, file_location: str):

    print("HD: importing entries")
    for chunk in pd.read_fwf(file_location, chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES):

        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))

        query_string = "WITH $batch as batch " \
            "UNWIND batch as item " \
            "CREATE (s:star) s+= item " \
            "RETURN id(s) as id"
        conn = Neo4jConnection(uri, user, password)
        result = conn.query(query_string, parameters={'batch':batch})

        if response != None and response[0] != None:
            id_list = []
            for record in response:
                id_list.append(record.get('id'))

            query_string = "WITH $idlist as id_list " \
                "UNWIND id_list as item " \
                "MATCH (c:CATALOG), (s:STAR) " \
                "WHERE c.name = 'HD' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            response = conn.query(query_string, parameters={'idlist': id_list})

def convert_row_to_dict(row) -> Dict:

    ra = "{}h{}m".format(row.RAh, row.RAdm)
    dec = "{}{}d{}m".format(row.DE_, row.DEd, row.DEm)

    sky_coord = SkyCoord(ra=ra, dec=dec, frame='fk4', equinox='B1900')
    #sky_coord = SkyCoord(ra=row.RAdm, dec=dec, unit=(u.hour, u.deg), frame='B1900')

    result = {
        'catalog': 'HD',
        'name': "HD " + row.HD,
        'number':row.HD,
        'dm': row.DM,
        'ra': ra,
        'ra_dec': round(sky_coord.ra.deg, 3),
        'dec': dec,
        'dec_dec': round(sky_coord.dec.deg, 3),
        'photovisual magnitude': row.Ptm,
        'photographic magnitude': row.Ptg,
        'spectral type full': row.SpT,
        'intensity': row.Int,
        'remarks': row.Rem if not np.nan else None
    }

if __name__ == "__main__":

    do_hd()