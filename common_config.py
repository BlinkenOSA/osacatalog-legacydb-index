import mysql.connector
import pysolr

config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': 'catalog_import',
}

con = mysql.connector.connect(**config)

solr_interface = pysolr.Solr("http://localhost:8983/solr/osacatalog")
