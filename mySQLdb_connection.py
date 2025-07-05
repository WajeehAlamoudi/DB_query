import os
import mysql.connector
from mysql.connector import Error
from sqlalchemy.engine import make_url


# # Change the credentials as per your local setup
# HOST = os.getenv("MYSQL_HOST")
# USER = os.getenv("MYSQL_USER")
# PASSWORD = os.getenv("MYSQL_PASS")
# DATABASE = os.getenv("MYSQL_DB")
#
#
# def create_mysql_connection(host_name, user_name, user_password, db_name):
#     """Establishes connection to the MySQL database."""
#     connection = None
#     try:
#         connection = mysql.connector.connect(
#             host=host_name,
#             user=user_name,
#             password=user_password,
#             database=db_name
#         )
#         if connection.is_connected():
#             print(f"connection ok")
#     except Error as e:
#         print(f"Error: {e}")
#     return connection
#
#
# def close_connection(connection):
#     """Closes the MySQL database connection."""
#     if connection and connection.is_connected():
#         connection.close()
#
#
# def get_connection():
#     return create_mysql_connection(HOST, USER, PASSWORD, DATABASE)
#

class MySQLClient:
    def __init__(self, uri=None):
        self.uri = uri or os.getenv("MYSQL_URI")
        self.connection = None

    def connect(self):
        """Establish MySQL connection."""
        try:
            url = make_url(self.uri)
            self.connection = mysql.connector.connect(
                host=url.host,
                user=url.username,
                password=url.password,
                database=url.database,
                port=url.port or 3306
            )
            if self.connection.is_connected():
                print("Connected to MySQL.")
                return self.connection
        except Error as e:
            print(f"❌ MySQL connection error: {e}")
            self.connection = None

        return None  # Only return None if connection failed

    def disconnect(self):
        try:
            if self.connection:
                self.connection.cmd_reset_connection()
                self.connection.close()
                print("🔌 Disconnected.")
        except Error as e:
            print(f"⚠️ Disconnect error: {e}")

    def execute_query(self, query, params=None):
        """Run a query (SELECT/INSERT/UPDATE)."""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
                return result
            self.connection.commit()
            return cursor.rowcount
        except Error as e:
            print(f"⚠️ Query failed: {e}")
            return None
        finally:
            cursor.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
