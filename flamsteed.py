from neo4j_connection import Neo4jConnection
import pandas as pd
import numpy as np
from BaseCatalog import BaseCatalog
from typing import Dict
from util import get_constellation, get_greek_letter, HMS2deg, DMS2deg
import math

class Flamsteed(BaseCatalog):



    def __init__(self, db_uri: str = None, user: str = None, password: str = None, file_location: str=None):

        BaseCatalog.__init__(self)

        self.__db_uri = db_uri
        self.__user = user
        self.__password = password
        self.__file_location = file_location

    def create_catalog(self):

        pass
        # conn = Neo4jConnection(self.__uri, self.__user, self.__password)
        # query_string = "CREATE (c:CATALOG) " \
        #     "SET c.name='Flamesteed', c.epoch=1690, c.author='Flamsteed, Lalande'"
        # response = conn.query(query_string)
        # print(response)
        # conn.close()

    def import_entries(self):

        names = ["FNo", "FCon", "FNum","BCon","BLet","BInd","Mag", "AR_d","AR_m","AR_s","DP_d","DP_m","DP_s"]
        colspecs = [(0,4),(5,8),(9,12),(34,37),(38,41),(42,43),(100,103),(44,47),(48,50),(51,53),(54,57),(58,60),(61,63)]

        #for chunk in pd.read_fwf(self.__file_location, chunksize = 100, colspecs='infer', names=names):
        for chunk in pd.read_fwf(self.__file_location, chunksize = 100, colspecs=colspecs, names=names):
            # print(chunk)
            for row in chunk.itertuples():
                entry = Flamsteed.convert_row_to_dict(row);
                print(entry)

    def convert_row_to_dict(row):

        bayer_name = "{} {}".format(get_greek_letter(str(row.BLet)), get_constellation(str(row.BCon)))
        if bayer_name == "None None":
            bayer_name = None

        result = {
            "full_name": "{} {}".format(str(row.FNum), get_constellation(row.FCon)),
            "name": "{} {}".format(row.FNum, row.FCon),
            "constellation": row.FCon,
            "number": row.FNum,
            "bayer_name": bayer_name,
            "bayer_const": row.BCon if bayer_name != None else None,
            "bayer_letter": row.BLet if bayer_name != None else None,
            "bayer_index": row.BInd if bayer_name != None else None,  # this still doesn't work
            "ra": {
                "epoch": 1690,
                "ra": str(row.AR_d) + " " + str(row.AR_m) + " " + str(row.AR_s),
                "ra_deg": DMS2deg(str(row.AR_d) + " " + str(row.AR_m) + " " + str(row.AR_s), scale=4),
            },
            "dec": {
                "epoch": 1690,
                "dec": str(row.DP_d) + " " + str(row.DP_m) + " " + str(row.DP_s),
                "dec_deg": DMS2deg(str(row.DP_d) + " " + str(row.DP_m) + " " + str(row.DP_s), scale=4)
            },
            "magnitude": row.Mag
        }

        print(type(row.BInd))

        # get rid of nan in bayer_index
        if result["bayer_index"] != result["bayer_index"]:
            result["bayer_index"] = None


        return result

    def write_dict_to_neo4j(input: Dict):
        pass


if __name__ == "__main__":
    instance = Flamsteed(db_uri="bolt://localhost:7687", user="neo4j", password="(IJN8uhb", file_location="flamsteed_l.dat")
    instance.create_catalog()
    instance.import_entries()
