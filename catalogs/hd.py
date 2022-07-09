from typing import Dict
import time
import pandas as pd
import configparser
from math import modf, floor
from astropy.coordinates import SkyCoord
from astropy import units as u
import numpy as np
import os, sys, inspect
from urllib.request import urlretrieve

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from util import get_greek_letter, get_constellation, DMS2deg
from config.config import URI, USER, PASSWORD

COLUMN_NAMES = ['HD','DM','RAh','RAdm','DE_','DEd','DEm','Ptm','Ptg','SpT','Int','Rem']
COLSPECS = [(0,6),(6,18),(18,20),(20,23),(23,24),(24,26),(26,28),(29,34),(36,41),(42,45),(45,47),(47,48)]

def do_hd():
    start_time = time.time()

    import_entries(URI, USER, PASSWORD, 'https://cdsarc.cds.unistra.fr/ftp/III/135A/catalog.dat.gz')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("HD: catalog imported in {} seconds".format(round(total_time, 3)))

def import_entries(uri: str, user:str, password:str, file_location: str):

    print("HD: importing entries")

    if not os.path.exists('/tmp/hd_catalog.gz'):
        print("HD: catalog file not found locally, downloading")
        urlretrieve(url=file_location, filename="/tmp/hd_catalog.gz")[0]
        print("HD: Catalog downloaded")
    else:
        print("HD: found cached hd_catalog.gz")

    for chunk in pd.read_fwf("/tmp/hd_catalog.gz", chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES):

        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))

        # for item in batch:
        #     query_string = "CALL {CREATE (s:STAR) set s = $item return id(s) as id} IN TRANSACTIONS OF 1000 ROWS"
        #     pass

        query_string = "WITH $batch as batch " \
            "UNWIND batch as item " \
            "CREATE (s:STAR) SET s+= item " \
            "RETURN id(s) as id"
        conn = Neo4jConnection(uri, user, password)
        response = conn.query(query_string, parameters={'batch':batch})

        if response != None and response[0] != None:
            id_list = []
            for record in response:
                id_list.append(record.get('id'))

            # query_string = "MATCH (c:CATALOG), (s:STAR) where c.name = 'HD' and id(s) in $idlist " \
            #     "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
            #     "return ce"
            query_string = "WITH $idlist as id_list " \
                "UNWIND id_list as item " \
                "MATCH (c:CATALOG), (s:STAR) " \
                "WHERE c.name = 'HD' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            response = conn.query(query_string, parameters={'idlist': id_list})

def convert_row_to_dict(row) -> Dict:

    ra = "{}h{}m".format(row.RAh, row.RAdm)
    dec = "{} {}d{}m".format(row.DE_, row.DEd, row.DEm)

    result = {
        'catalog': 'HD',
        'name': "HD " + str(row.HD),
        'number':row.HD,
        'dm': row.DM,
        'ra': ra,
        'dec': dec,
        'photovisual magnitude': row.Ptm,
        'photographic magnitude': row.Ptg,
        'spectral type full': row.SpT,
        'intensity': row.Int,
        'remarks': row.Rem if not np.nan else None
    }

    return result

if __name__ == "__main__":

    do_hd()