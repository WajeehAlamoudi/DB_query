from mySQLdb_connection import MySQLClient
from mySQLdb_schema_extractor import start_mysql_documentation
from enricher_gpt import enrich_the_DB_schema


def documenting_mySQL_DB():
    db = MySQLClient()
    conn = db.connect()

    if conn:
        embed_file, summary_file = start_mysql_documentation(conn)
        print(f"📄 Embed file: {embed_file}")
        print(f"📄 Summary file: {summary_file}")
        db.disconnect()
    else:
        print("❌ Failed to connect.")
    return embed_file, summary_file


if __name__ == "__main__":
    embed_file_path, _ = documenting_mySQL_DB()
    enrich_the_DB_schema(embed_file_path)
