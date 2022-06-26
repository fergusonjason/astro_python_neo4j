from neo4j_connection import Neo4jConnection
import configparser

def init(uri, user, password):
    conn = Neo4jConnection(uri, user, password)
    query_string = "MATCH (n) DETACH DELETE n"
    response = conn.query(query_string)
    conn.close()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    uri = config['Database']['uri']
    user = config['Database']['user']
    password = config['Database']['password']

    init(uri, user, password)