from typing import Dict, Any
import os, sys, inspect
import pandas as pd
import time
from urllib.request import urlretrieve

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from util import DMS2deg, HMS2deg
from config.config import URI, USER, PASSWORD

COLUMN_NAMES = ['identifier','RAhms','DEdms','Vmag','VarFlag','RAdeg','DEdeg','Plx','pmRA','pmDE','BTmag','VTmag','B_V','V_I','Period','MultFlag','m_HIP','Chart','HD','BD','CoD','CPD','SpType']
COLSPECS = [(8,14),(17,28),(29,40),(41,46),(47,48),(51,63),(64,76),(79,86),(87,95),(96,104),(217,223),(231,236),(245,251),(260,264),(313,320),(346,347),(352,354),(386,387),(391,396),(397,407),(408,418),(419,429),(435,447)]

def import_entries(uri: str, user:str, password:str, file_location: str):

    print("Hipparcos: importing entries")
    if not os.path.exists('/tmp/hipparcos.dat'):
        print("Hipparcos: file not found, downloading")
        urlretrieve(url=file_location, filename="/tmp/hipparcos.dat")[0]
    else:
        print("Hipparcos: found cached hipparcos.dat")

    for chunk in pd.read_fwf("/tmp/hipparcos.dat", chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES):

        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))

        query_string = "WITH $batch as batch " \
            "UNWIND batch as item " \
            "CREATE (s:STAR) SET s += item " \
            "RETURN id(s) as id"
        conn = Neo4jConnection(uri, user, password)
        response = conn.query(query_string, parameters={'batch': batch})
        conn.close()

        if response != None and response[0] != None:
            id_list = []
            for record in response:
                id_list.append(record.get('id'))

            query_string = "WITH $idlist as id_list " \
                "UNWIND id_list as item " \
                "MATCH (c:CATALOG), (s:STAR) " \
                "WHERE c.name = 'Hipparcos' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            response = conn.query(query_string, parameters={'idlist': id_list})
            conn.close()

def convert_row_to_dict(row) -> Dict[str, Any]:

    result = {
        'catalog': 'Hipparcos',
        'name': "HIP " + str(row.identifier),
        'ra': row.RAhms,
        'ra_dec': row.RAdeg,
        'dec': row.DEdms,
        'dec_dec': row.DEdeg,
        'visual magnitude': row.Vmag,
        'variability': row.VarFlag if not None else None,
        'parallax': row.Plx,
        'proper motion ra': row.pmRA,
        'proper motion dec': row.pmDE,
        'BT magnitude': row.BTmag if not None else None,
        'VT magnitude': row.VTmag if not None else None,
        'BV color': row.B_V,
        'VI color': row.V_I,
        'period': row.Period if not None else None,
        'multiple flag': row.MultFlag if not None else None,
        'component identifiers': row.m_HIP if not None else None,
        'identification chart': row.Chart if not None else None,
        'HD number': row.HD if not None else None,
        'BD': row.BD if not None else None,
        'CoD': row.CoD if not None else None,
        'CPD': row.CPD if not None else None,
        'spectral type': row.SpType
    }

    return result

def do_hipparcos():

    start_time = time.time()

    import_entries(URI, USER, PASSWORD, 'https://cdsarc.cds.unistra.fr/ftp/I/239/hip_main.dat')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("Hipparcos: catalog imported in {} seconds".format(total_time))

if __name__ == "__main__":
    do_hipparcos()