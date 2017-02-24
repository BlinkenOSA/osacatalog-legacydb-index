import mysql.connector
import sunburnt

config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Vizipoki304',
    'database': 'catalog_import',
}

con = mysql.connector.connect(**config)

solr_interface = sunburnt.SolrInterface("http://localhost:8983/solr/osacatalog")
