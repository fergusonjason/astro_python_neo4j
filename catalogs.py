# from catalogs.neo4j_connection.neo4j_connection import Neo4jConnection
import time
from typing import Dict
import os, sys, inspect

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from config.config import URI, USER, PASSWORD

def do_catalogs():

    start_time = time.time()
    print('Creating catalogs')


    # create individual catalogs
    # TODO: convert this to calling a List of functions
    catalogs = []
    catalogs.append(create_gliese_dict())
    catalogs.append(create_flamsteed_dict())
    catalogs.append(create_hd_dict())

    # write catalog nodes
    query_string = "WITH $catalogs as catalogs " \
        "UNWIND catalogs as catalog " \
        "CREATE (c:CATALOG) set c += catalog " \
        "RETURN c.name, id(c) as id"
    conn = Neo4jConnection(db_uri=URI, user=USER, password=PASSWORD)
    conn.query(query=query_string, parameters={'catalogs': catalogs})
    conn.close()

    # create unique constraint on catalog name, don't need an index apparently
    query_string = "CREATE CONSTRAINT catalog_name_unique IF NOT EXISTS FOR (catalog:CATALOG) REQUIRE catalog.name IS UNIQUE";
    conn = Neo4jConnection(db_uri=URI, user=USER, password=PASSWORD)
    conn.query(query=query_string, parameters={'catalogs': catalogs})
    conn.close()

    # create index on name
    # query_string = "CREATE INDEX catalog_name_idx IF NOT EXISTS FOR (catalog:CATALOG) ON (catalog.name)"
    # conn = Neo4jConnection(db_uri=URI, user=USER, password=PASSWORD)
    # conn.query(query_string)

    end_time = time.time()
    total_time = end_time - start_time
    print("Created catalogs in {} seconds".format(round(total_time, 3)))

def create_flamsteed_dict() -> Dict[str,str]:

    return {
        'name':'Flamsteed',
        'catalog full name': '',
        'catalog author': 'Flamsteed, Lalande'
    }

def create_gliese_dict() -> Dict[str, str]:

    return {
        'name': 'Gliese',
        'catalog full name': 'Preliminary Version of the Third Catalogue of Nearby Stars',
        'catalog author': 'Gliese W., Jahreiss H.'
    }

def create_hd_dict():

    return {
        'name': 'HD',
        'catalog full name': 'Henry Draper Catalogue and Extension (Cannon+ 1918-1924; ADC 1989)',
        'catalog author': 'Cannon A.J., Pickering E.C.'
    }

if __name__ == '__main__':
    do_catalogs()

