
from sqlite3 import connect
from neo4j_connection import Neo4jConnection


STELLAR_CLASSES = ("O","B","A","F","G","K","M")

if __name__ == "__main__":
    for stellar_class in STELLAR_CLASSES:
        connection =  Neo4jConnection(db_uri="bolt://localhost:7687", user="neo4j", password="(IJN8uhb")
        query_string = "MATCH (sc: STELLAR_CLASS {class : '" + stellar_class + "'}) WITH COUNT(sc) > 0 as node_exists RETURN node_exists"
        response = connection.query(query=query_string)
        connection.close()
        if response[0].get("node_exists") == False:
            query_string = "CREATE (sc:STELLAR_CLASS) " \
                "SET sc.class = '{}'".format(stellar_class)
            connection.query(query_string)
            connection.close()



