import sys, os, inspect
from typing import Dict, Tuple, List
import time
from urllib.request import urlretrieve
import pandas as pd
import numpy as np
import math
#from catalogs.hd import COLUMN_NAMES

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from config.config import URI, USER, PASSWORD

# it's an extra step but this way I can check if the colspecs don't match the key easier
COLUMN_DICT = {
    "number": (0,6),
    "RAh": (7,9),
    "RAm": (9,11),
    "RAs": (11,17),
    "pmRA": (17,24),
    "DE_": (41,42),
    "DEd": (42,44),
    "DEm": (44,46),
    "DEs": (46,51),
    "pmDE": (51,57),
    "Ptm": (76,80),
    "VMag": (80,84),
    "SpType": (84,87),
    "DM": (104,117),
    "HD": (117,123),
    "Boss": (124,129)
}

COLUMN_NAMES = []
COLSPECS = []
for key in COLUMN_DICT:
    COLUMN_NAMES.append(key)
    COLSPECS.append(COLUMN_DICT[key])

def do_sao():

    start_time = time.time()

    import_entries(file_location='https://cdsarc.cds.unistra.fr/ftp/I/131A/sao.dat.gz', uri= URI, user = USER, password = PASSWORD)

    end_time = time.time()
    total_time = round(end_time - start_time, 3)

    print("SAO: Catalog imported in {} seconds".format(total_time))


def import_entries(file_location: str, uri: str, user: str, password: str):

    print("SAO: importing entries")

    if not os.path.exists('/tmp/sao_catalog.gz'):
        print("SAO: catalog file not found locally, downloading")
        urlretrieve(url=file_location, filename="/tmp/sao_catalog.gz")[0]
        print("SAO: Catalog downloaded")
    else:
        print("SAO: found cached sao_catalog.gz")

    for chunk in pd.read_fwf('/tmp/sao_catalog.gz', chunksize = 10000, colspecs=COLSPECS, names=COLUMN_NAMES):

        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))


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

            query_string = "WITH $idlist as id_list " \
                "UNWIND id_list as item " \
                "MATCH (c:CATALOG), (s:STAR) " \
                "WHERE c.name = 'SAO' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            response = conn.query(query_string, parameters={'idlist': id_list})



def convert_row_to_dict(row):

    HD = None
    if not math.isnan(row.HD):
        HD = row.HD
        HD = int(HD)

    boss = None
    if not math.isnan(row.Boss):
        boss = row.Boss
        boss = int(boss)

    result = {
        "catalog": "SAO",
        "name": "SAO " + str(row.number),
        "number": row.number,
        "ra": "{}h{}m{}s".format(row.RAh, row.RAm, round(row.RAs,3)),
        "dec": "{}{} {}m{}s".format(row.DE_, row.DEd, row.DEm, row.DEs),
        "proper motion ra": row.pmRA,
        "photographic magnitude": row.Ptm if not 99.9 else None, # 99.9 is a holder value for "unknown"
        "visual magnitude": row.VMag if not 99.9 else None, # 99.9 is a holder value for "unknown"
        "spectral type": row.SpType if not np.nan else None,
        "DM": row.DM if not np.nan else None,
        "HD": HD,
        "Boss": row.Boss if not np.nan else None
    }

    return result

if __name__ == "__main__":

    do_sao()