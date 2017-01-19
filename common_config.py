import mysql.connector
import sunburnt

config = {
    'host': 'localhost',
    'port': 3306,
    'user': '',
    'password': '',
    'database': '',
}

con = mysql.connector.connect(**config)

solr_interface = sunburnt.SolrInterface("http://localhost:8983/solr")
