# from catalogs.neo4j_connection.neo4j_connection import Neo4jConnection
import time
from typing import Dict
import os, sys, inspect
from urllib.parse import ParseResultBytes

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from config.config import URI, USER, PASSWORD

def do_catalogs(catalog_functions):

    start_time = time.time()
    print('Creating catalogs')

    catalogs = []
    for f in catalog_functions:
        catalogs.append(f())
        pass

    # create individual catalogs
    # TODO: convert this to calling a List of functions
    # catalogs = []
    # catalogs.append(create_gliese_dict())
    # catalogs.append(create_flamsteed_dict())
    # catalogs.append(create_hd_dict())

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
        'catalog author': 'Gliese W., Jahreiss H.',
        'location': 'https://cdsarc.cds.unistra.fr/ftp/V/70A/catalog.dat.gz'
    }

def create_hd_dict():

    return {
        'name': 'HD',
        'catalog full name': 'Henry Draper Catalogue and Extension (Cannon+ 1918-1924; ADC 1989)',
        'catalog author': 'Cannon A.J., Pickering E.C.',
        'location': 'https://cdsarc.cds.unistra.fr/ftp/III/135A/catalog.dat.gz'
    }

def create_hipparcos_dict():

    return {
        'name': 'Hipparcos',
        'catalog full name': '',
        'location': 'https://cdsarc.cds.unistra.fr/ftp/I/239/hip_main.dat'
    }

def create_hr():

    return {
        'name':'HR',
        'catalog full name':'Bright Star Catalogue, 5th Revised Ed.',
        'location':'https://cdsarc.cds.unistra.fr/ftp/V/50/catalog.gz'
    }

def create_common_names():

    return {
        'name':'Common Names',
        'catalog full name': '',
        'location':'https://raw.githubusercontent.com/mirandadam/iau-starnames/master/catalog_data/IAU-CSN.txt'
    }

def create_sao():

    return {
        'name':'SAO',
        'catalog full name': 'Smithsonian Astrophysical Observatory Star Catalog,  version 1989',
        'location': 'https://cdsarc.cds.unistra.fr/ftp/I/131A/sao.dat.gz'
    }

catalogs_to_import = [create_flamsteed_dict, create_gliese_dict, create_hd_dict, create_hipparcos_dict, create_hr, create_common_names, create_sao]

if __name__ == '__main__':

    # catalogs_to_import = [create_flamsteed_dict, create_gliese_dict, create_hd_dict, create_hipparcos_dict]
    do_catalogs(catalogs_to_import)

