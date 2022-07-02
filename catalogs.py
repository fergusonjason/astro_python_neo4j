from neo4j_connection import Neo4jConnection
import time
from typing import Dict
import configparser

URI = ""
USER = ""
PASSWORD = ""

def do_catalogs():

    start_time = time.time()
    print('Creating catalogs')

    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    URI = config['Database']['uri']
    USER = config['Database']['user']
    PASSWORD = config['Database']['password']

    # create individual catalogs
    catalogs = []
    catalogs.append(create_gliese_dict())
    catalogs.append(create_flamsteed_dict())

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

    # create the unique index

def create_hd():
    pass

if __name__ == '__main__':
    do_catalogs()

