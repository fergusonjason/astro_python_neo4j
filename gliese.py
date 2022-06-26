
from flamsteed import COLSPECS
from neo4j_connection import Neo4jConnection
from typing import List
from typing import Tuple

COLUMN_NAMES = ['Name','Comp','RAh','RAm', 'RAs','DE_','DEd','DEm','pm','pmPA','RV','Sp','VMag','BV','U_B','R_I','trplx','e_trplx','plx','e_plx','Mv','HD','DM','Giclas','LHS','OtherName']
COLSPECS: List[Tuple[int,int]] = [(0,8),(8,10),(12,14),(15,17),(18,20),(21,22),(22,24),(25,29),(30,36),(37,42),(43,49),(54,66),(67,73),(75,80),(82,87),(89,94),
    (96,102),(102,107),(108,114),(114,119),(121,126),(146,152),(153,165),(168,175),(176,181),(182,187)]

CATALOG_NAME = 'Gliese'
CATALOG_FULL_NAME = 'Preliminary Version of the Third Catalogue of Nearby Stars'
CATALOG_AUTHOR = 'Gliese W., Jahreiss H.'

def create_catalog(uri, user, password):
    query_string = "MATCH (catalog: CATALOG {name: '$catalog_name'}) WITH COUNT(catalog) > 0 as node_exists RETURN node_exists"
    conn = Neo4jConnection(uri, user, password)
    result = conn.query(query_string)
    conn.close()
    catalog_exists = False
    if result != None and result[0] != None:
        catalog_exists = result[0].get("node_exists")

    if catalog_exists == False:
        query_string = "CREATE (c:CATALOG) " \
            "SET c.name='$catalog_name', c.epoch=1950, c.author='$catalog_author', c.full_name='$catalog_full_name'"
        conn = Neo4jConnection(uri, user, password)
        result = conn.query(query_string, parameters={'catalog_name': CATALOG_NAME, 'catalog_author': CATALOG_AUTHOR, 'catalog_full_name': CATALOG_FULL_NAME})
        conn.close()
    pass

def import_entries(file_location: str):
    pass

if __name__ == '__main__':
    if len(COLUMN_NAMES) != len(COLSPECS):
        print("length of column name {} does not match length of colspecs {}".format(len(COLUMN_NAMES), len(COLSPECS)))
    pass
