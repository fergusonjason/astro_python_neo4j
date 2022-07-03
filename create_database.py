from init import do_init
from catalogs import do_catalogs
from catalogs.flamsteed import do_flamsteed
from catalogs.gliese import do_gliese
from catalogs.hd import do_hd
# from config.config import URI, USER, PASSWORD
# from neo4j_connection import Neo4jConnection
from time import time
import os, sys, inspect

# python rediculousness
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from config.config import URI, USER, PASSWORD
from neo4j_connection import Neo4jConnection


def do_init():

    start_time = time.time()

    conn = Neo4jConnection(URI, USER, PASSWORD)
    query_string = "MATCH (n) DETACH DELETE n"
    response = conn.query(query_string)
    conn.close()

    query_string = "SHOW CONSTRAINTS"
    conn = Neo4jConnection(URI, USER, PASSWORD)
    response = conn.query(query_string)

    for record in response:
        constraint_name = record.get('name')
        query_string = "DROP CONSTRAINT " + constraint_name + " IF EXISTS"
        conn.query(query_string)

    query_string = "SHOW INDEXES"
    conn = Neo4jConnection(URI, USER, PASSWORD)
    response = conn.query(query_string)

    for record in response:
        index_name = record.get('name')
        query_string = "DROP INDEX " + index_name + " IF EXISTS"
        conn.query(query_string)

    end_time = time.time()
    total_time = round(end_time - start_time, 3)
    print("Database truncated in {} seconds".format(total_time))



if __name__ == "__main__":
    do_init()
    do_catalogs()
    # do_flamsteed()
    # do_gliese()
    # do_hd()