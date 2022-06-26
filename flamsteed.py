from neo4j_connection import Neo4jConnection
import configparser
import pandas as pd
from util import get_greek_letter, get_constellation, DMS2deg
from typing import Dict

EXTERNAL_FILE = "http://pbarbier.com/flamsteed/flamsteed_l.dat"

COLUMN_NAMES = ["FNo", "FCon", "FNum","BCon","BLet","BInd","Mag", "AR_d","AR_m","AR_s","DP_d","DP_m","DP_s"]
COLSPECS = [(0,4),(5,8),(9,12),(34,37),(38,41),(42,43),(100,103),(44,47),(48,50),(51,53),(54,57),(58,60),(61,63)]

CATALOG_NAME = "Flamsteed"
CATALOG_AUTHOR = "Flamsteed, Lalande"

DRY_RUN: bool = False

def create_catalog(uri, user, password):

    query_string = "MATCH (catalog: CATALOG {name: '$catalog_name'}) WITH COUNT(catalog) > 0 as node_exists RETURN node_exists"
    if DRY_RUN == True:
        print(query_string)
    else:
        conn = Neo4jConnection(uri, user, password)

        response = conn.query(query=query_string, parameters = {'catalog_name': "CATALOG_NAME"})
        node_exists = None
        if response != None and response[0] != None:
            node_exists = response[0].get("node_exists")

        if node_exists == True:
            print("Catalog already exists")
            return
        else:
            print("Creating catalog")
        conn.close()

    query_string = "CREATE (c:CATALOG) " \
        "SET c.name='Flamsteed', c.epoch=1690, c.author='Flamsteed, Lalande'"
    if DRY_RUN == True:
        print(query_string)
    else:
        response = conn.query(query_string)
        conn.close()

def import_entries(file_location: str):

    print("Importing Flamsteed entries")
    for chunk in pd.read_fwf(file_location, chunksize = 100, colspecs=COLSPECS, names=COLUMN_NAMES):
        for row in chunk.itertuples():
            # create data
            entry = convert_row_to_dict(row);

            # write data to neo4j
            query_string='CREATE (s:STAR) set s = $entry'
            if DRY_RUN == True:
                print("DRY RUN: {}".format(entry))
            else:
                conn = Neo4jConnection(uri, user, password)
                response = conn.query(query_string, parameters={'entry': entry})
                conn.close()

            # create connection to catalog node
            query_string = "MATCH (c:CATALOG), (s: STAR) " \
                "WHERE c.name = 'Flamsteed' and s.full_name = $full_name " \
                "CREATE (s)-[ce:CATALOG_ENTRY] -> (c) " \
                "RETURN type(ce)"
            if DRY_RUN == True:
                print("DRY RUN: {}".format(query_string))
            else:
                conn = Neo4jConnection(uri, user, password)
                response = conn.query(query_string, parameters={'full_name': entry.get('full_name')})
                conn.close()


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

def create_relationships(uri, user, password):
    query_string = "MATCH (c:CATALOG), (s:STAR) WHERE s.catalog='Flamsteed' return s"
    conn = Neo4jConnection(uri, user, password)
    response = conn.query(query_string)
    conn.close()
    pass


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    uri = config['Database']['uri']
    user = config['Database']['user']
    password = config['Database']['password']

    create_catalog(uri, user, password)
    import_entries('http://pbarbier.com/flamsteed/flamsteed_l.dat')
