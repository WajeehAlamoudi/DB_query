from config import *
from mySQLdb_connection import MySQLClient
from tools import *

mysql_client = MySQLClient()

connexction = mysql_client.connect()

print(get_embed_path(connexction))