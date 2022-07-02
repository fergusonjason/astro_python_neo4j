from neo4j_connection import Neo4jConnection
import configparser
import time


def do_init():
    start_time = time.time()
    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    uri = config['Database']['uri']
    user = config['Database']['user']
    password = config['Database']['password']

    init(uri, user, password)

    end_time = time.time()
    total_time = round(end_time - start_time, 3)
    print("Database truncated in {} seconds".format(total_time))

def init(uri, user, password):
    conn = Neo4jConnection(uri, user, password)
    query_string = "MATCH (n) DETACH DELETE n"
    response = conn.query(query_string)
    conn.close()

    query_string = "SHOW CONSTRAINTS"
    conn = Neo4jConnection(uri, user, password)
    response = conn.query(query_string)

    for record in response:
        constraint_name = record.get('name')
        query_string = "DROP CONSTRAINT " + constraint_name + " IF EXISTS"
        conn.query(query_string)

    query_string = "SHOW INDEXES"
    conn = Neo4jConnection(uri, user, password)
    response = conn.query(query_string)

    for record in response:
        index_name = record.get('name')
        query_string = "DROP INDEX " + index_name + " IF EXISTS"
        conn.query(query_string)



if __name__ == "__main__":

    do_init()
