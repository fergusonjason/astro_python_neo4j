from typing import Dict, Any
import os, sys, inspect
import pandas as pd
import time
from urllib.request import urlretrieve
from typing import Tuple, List
import numpy as np

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from util import DMS2deg, HMS2deg
from config.config import URI, USER, PASSWORD

CATALOG_NAME = 'HR'
CATALOG_FULL_NAME = 'Bright Star Catalogue, 5th Revised Ed.'
CATALOG_AUTHOR = 'Hoffleit D., Warren Jr W.H.'

COLUMN_NAMES = ['HR','Name','DM','HD','SAO','FK5','Multiple','ADS','VarID','RAh','RAm','RAs',
    'DE_','DEd','DEm','DEs','GLON','GLAT','Vmag','B_V','U_B','R_I','SpType','pmRA','pmDE','Parallax','RadVel','RotVel']

COLSPECS: List[Tuple[int,int]] = [(0,4),(4,14),(14,25),(25,31),(31,37),(37,41),(43,44),(44,49),(49,51),(51,60),
    (75,77),(77,79),(79,83),(83,84),(84,86),(86,88),(88,90),(90,96),(96,102),(102,107),(109,114),(115,120),(121,126),
    (127,147),(148,154),(154,160),(161,166),(166,170),(176,179)]

def do_hr():

    start_time = time.time()

    import_entries(URI, USER, PASSWORD, 'https://cdsarc.cds.unistra.fr/ftp/V/50/catalog.gz')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("HR: catalog imported in {} seconds".format(total_time))

def import_entries(uri: str, user:str, password:str, file_location: str):

    print("HR: importing entries")

    if not os.path.exists('/tmp/hr_catalog.gz'):
        print("HR: catalog file not found locally, downloading")
        urlretrieve(url=file_location, filename="/tmp/hr_catalog.gz")[0]
        print("HR: Catalog downloaded")
    else:
        print("HR: found cached hr_catalog.gz")

    for chunk in pd.read_fwf('/tmp/hr_catalog.gz', chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES):

        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))

        query_string = "WITH $batch as batch " \
            "UNWIND batch as item " \
            "CREATE (s:star) SET s+= item " \
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
                "WHERE c.name = 'HR' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            response = conn.query(query_string, parameters={'idlist': id_list})
    pass

def convert_row_to_dict(row) -> Dict[str, Any]:

    result = {
        'catalog': 'HR',
        'name': "HR " + str(row.HR),
        'number': row.HR if not np.nan else None,
        'full name': row.Name,
        'DM': row.DM,
        'HD': row.HD,
        'SAO': row.SAO,
        'FK5': row.FK5,
        'multiple': row.Multiple,
        'ADS': row.ADS,
        'variable star identification': row.VarID,
        'ra': "{}h{}m{}s".format(row.RAh, row.RAm, row.RAs),
        'dec': "{}{} {}m{}s".format(row.DE_, row.DEd, row.DEm, row.DEs),
        'GLON': row.GLON,
        'GLAT': row.GLAT,
        'visual magnitude': row.Vmag,
        'BV color': row.B_V,
        'UB color': row.U_B,
        'RI color': row.R_I,
        'spectral type': row.SpType,
        'proper motion ra': row.pmRA,
        'proper motion dec': row.pmDE,
        'parallax': row.Parallax,
        'radial velocity': row.RadVel,
        'rotational velocity': row.RotVel
    }

    return result

if __name__ == "__main__":

    do_hr()