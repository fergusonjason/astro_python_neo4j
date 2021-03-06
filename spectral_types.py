import configparser
import time
import os, sys, inspect

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from neo4j_connection import Neo4jConnection
from config.config import URI, USER, PASSWORD


def do_spectral_types():
    start_time = time.time()

    make_spectral_types()
    create_unqiue_contraint()

    end_time = time.time()
    total_time = end_time - start_time
    print("Spectral types created in {} seconds".format(round(total_time, 3)))

def make_spectral_types():

    spectral_types = []
    for item in ['O','B','A','F','G','K','M']:
        node = {
            "name": item
        }
        spectral_types.append(node)

    query_string = "WITH $spectral_types as spectral_types " \
        "UNWIND spectral_types as item " \
        "CREATE (st:SPECTRAL_TYPE) set st += item"
    conn = Neo4jConnection(URI, USER, PASSWORD)
    result = conn.query(query_string, parameters={'spectral_types': spectral_types})


def create_unqiue_contraint():
    query_string = "CREATE CONSTRAINT spec_type_unique ON (st:SPECTRAL_TYPE) ASSERT st.spectral_type IS UNIQUE"
    conn = Neo4jConnection(URI, USER, PASSWORD)
    result = conn.query(query=query_string)

if __name__ == "__main__":
    do_spectral_types()
