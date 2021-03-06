
from typing import List, Tuple, Dict, Any
import time
import pandas as pd
import configparser
import numpy as np
import os, sys, inspect

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from util import DMS2deg, HMS2deg
from config.config import URI, USER, PASSWORD

COLUMN_NAMES: List[str] = ['Name','Comp','RAh','RAm', 'RAs','DE_','DEd','DEm','pm','pmPA','RV','Sp','VMag','BV','UB','RI','trplx','e_trplx','plx','e_plx','Mv','HD','DM','Giclas','LHS','OtherName']
COLSPECS: List[Tuple[int,int]] = [(0,8),(8,10),(12,14),(15,17),(18,20),(21,22),(22,24),(25,29),(30,36),(37,42),(43,49),(54,66),(67,73),(75,80),(82,87),(89,94),
    (96,102),(102,107),(108,114),(114,119),(121,126),(146,152),(153,165),(168,175),(176,181),(182,187)]

def import_entries(uri: str, user:str, password:str, file_location: str):

    print("Gliese: importing entries")
    for chunk in pd.read_fwf(file_location, chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES):

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
                "WHERE c.name = 'Gliese' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            response = conn.query(query_string, parameters={'idlist': id_list})
            conn.close()


def convert_row_to_dict(row) -> Dict[str, Any]:

    ra = '{} {} {}'.format(str(row.RAh), str(row.RAm), str(row.RAm))
    ra_dec = HMS2deg(ra=ra, scale=3)
    dec = "{} {} {}".format(str(row.DEd), str(row.DEm), str('0'))

    dec_dec = DMS2deg(dec, 3)

    result = {
        'catalog': 'Gliese',
        'name': row.Name,
        'component': row.Comp if pd.notna(row.Comp) else None,
        'epoch': 1950,
        'ra': ra,
        'ra_dec': ra_dec,
        'dec': dec,
        'dec_dec': dec_dec,
        'proper motion': row.pm,
        'proper motion direction': row.pmPA,
        'radial velocity': row.RV if pd.notna(row.RV) else None,
        'spectral type full': row.Sp if pd.notna(row.Sp) else None, # this needs to be split into components
        'visual magnitude': row.VMag,
        'BV color': row.BV,
        'UB color': row.UB,
        'RI color': row.RI,
        'trigonometric parallax': row.trplx if pd.notna(row.trplx) else None,
        'trigonometric parallax error margin': row.e_trplx if pd.notna(row.e_trplx) else None,
        'resulting_parallax': row.plx if pd.notna(row.plx) else None,
        'resulting_parallax_error_margin': row.e_plx if pd.notna(row.e_plx) else None,
        'absolute visual magnitude': row.Mv,
        'HD number': int(row.HD) if pd.notna(row.HD) else None,
        'DM number': row.DM if pd.notna(row.DM) else None,
        'Giclass': row.Giclas if pd.notna(row.Giclas) else None,
        'LHS': row.LHS if pd.notna(row.LHS) else None,
        'Other name': row.OtherName if pd.notna(row.OtherName) else None
    }

    return result

def do_gliese():
    start_time = time.time()

    import_entries(URI, USER, PASSWORD, 'https://cdsarc.cds.unistra.fr/ftp/V/70A/catalog.dat.gz')
    end_time = time.time()
    total_time = round(end_time - start_time,3)
    print("Gliese: catalog imported in {} seconds".format(total_time))

if __name__ == '__main__':
    do_gliese()
