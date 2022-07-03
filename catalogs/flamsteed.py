import configparser
import pandas as pd
from typing import Dict
import time
import os, sys, inspect

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from util import get_greek_letter, get_constellation, DMS2deg
from config.config import URI, USER, PASSWORD

EXTERNAL_FILE = "http://pbarbier.com/flamsteed/flamsteed_l.dat"

COLUMN_NAMES = ["FNo", "FCon", "FNum","BCon","BLet","BInd","Mag", "AR_d","AR_m","AR_s","DP_d","DP_m","DP_s"]
COLSPECS = [(0,4),(5,8),(9,12),(34,37),(38,41),(42,43),(100,103),(44,47),(48,50),(51,53),(54,57),(58,60),(61,63)]

def do_flamsteed():

    start_time = time.time()

    import_entries(file_location='http://pbarbier.com/flamsteed/flamsteed_l.dat', uri= URI, user = USER, password = PASSWORD)

    end_time = time.time()
    total_time = round(end_time - start_time, 3)

    print("Flamsteed: Catalog imported in {} seconds".format(total_time))

def import_entries(file_location: str, uri: str, user: str, password: str):

    print("Flamsteed: Importing entries")
    for chunk in pd.read_fwf(file_location, chunksize = 1000, colspecs=COLSPECS, names=COLUMN_NAMES):
        batch = []
        for row in chunk.itertuples():
            batch.append(convert_row_to_dict(row))

        query_string = "WITH $batch as batch " \
            "UNWIND batch as item " \
            "CREATE (s:STAR) set s += item " \
            "return id(s) as id "
        conn = Neo4jConnection(uri, user, password)
        response = conn.query(query_string, parameters={'batch': batch})

        if response != None:

            id_list = []
            for record in response:
                id_list.append(record.get("id"))

            query_string = "WITH $idlist as id_list " \
                "UNWIND id_list as item " \
                "MATCH (c:CATALOG), (s:STAR) " \
                "WHERE c.name = 'Flamsteed' and id(s) = item " \
                "CREATE (s) - [ce:CATALOG_ENTRY] -> (c) " \
                "RETURN ce"
            conn = Neo4jConnection(uri, user, password)
            response = conn.query(query_string, parameters={'idlist': id_list})

def convert_row_to_dict(row):

    greek_letter = get_greek_letter(str(row.BLet))
    constellation = get_constellation(str(row.BCon))

    bayer_name = None
    if greek_letter != None and constellation != None:
        bayer_name = "{} {}".format(greek_letter, constellation)

    result : Dict = {
        'catalog': "Flamsteed",
        "full_name": "{} {}".format(str(row.FNum), get_constellation(row.FCon)),
        "name": "{} {}".format(row.FNum, row.FCon),
        "constellation": row.FCon,
        "number": row.FNum,
        "bayer_name": bayer_name,
        "bayer_const": row.BCon if bayer_name != None else None,
        "bayer_letter": row.BLet if bayer_name != None else None,
        "bayer_index": row.BInd if bayer_name != None else None,  # this still doesn't work
        "epoch": 1690,
        'ra': str(row.AR_d) + " " + str(row.AR_m) + " " + str(row.AR_s),
        'ra_decimal': DMS2deg(str(row.AR_d) + " " + str(row.AR_m) + " " + str(row.AR_s), scale=4),
        'dec': str(row.DP_d) + " " + str(row.DP_m) + " " + str(row.DP_s),
        'dec_decimal': DMS2deg(str(row.DP_d) + " " + str(row.DP_m) + " " + str(row.DP_s), scale=4),
        'magnitude': row.Mag
    }

    # get rid of nan in bayer_index
    if result["bayer_index"] != result["bayer_index"]:
        result["bayer_index"] = None


    return result


if __name__ == "__main__":

    do_flamsteed()
