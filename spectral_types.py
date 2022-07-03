import configparser
import time
from catalogs.neo4j_connection.neo4j_connection import Neo4jConnection


def do_spectral_types():
    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('app.properties')

    if config == None:
        print("No config found")
        quit()

    uri = config['Database']['uri']
    user = config['Database']['user']
    password = config['Database']['password']

    make_spectral_types(uri, user=user, password=password)
    create_unqiue_contraint(uri=uri, user=user, password=password)

    end_time = time.time()
    print("Spectral types imported in {} seconds".format(end_time - start_time))

def make_spectral_types(uri, user, password):

    spectral_types = ['O','B','F','G','K','M']

    query_string = "WITH $spectral_types as spectral_types " \
        "UNWIND spectral_types as spectral_type " \
        "CREATE (st:SPECTRAL_TYPE) set st += spectral_type"
    conn = Neo4jConnection(uri, user=user, password=password)
    result = conn.query(query_string, parameters={'spectral_types': spectral_types})

    # for spectral_type in spectral_types:



def create_unqiue_contraint(uri, user, password):
    query_string = "CREATE CONSTRAINT spec_type_unique ON (st:SPECTRAL_TYPE) ASSERT st.spectral_type IS UNIQUE"
    conn = Neo4jConnection(uri, user=user, password=password)
    result = conn.query(query=query_string)
    conn.close()

if __name__ == "__main__":
    do_spectral_types()
