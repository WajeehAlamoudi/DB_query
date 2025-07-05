import os
import json
from datetime import datetime
from mysql.connector import Error


def generate_output_paths(db_name):
    """
    Generate timestamped JSON output file paths inside a per-database folder.
    Example structure:
      prj root/
          └── db_name/
              ├── school_db_tables_summary_2025-06-22.json
              └── school_db_embed_format_2025-06-22.json
    """
    date_str = datetime.today().strftime("%Y-%m-%d")

    # Create a subdirectory for the specific database
    project_root = os.getcwd()

    db_folder = os.path.join(project_root, db_name)
    os.makedirs(db_folder, exist_ok=True)

    summary_file = os.path.join(db_folder, f"{db_name}_tables_summary_{date_str}.json")
    json_schema_file = os.path.join(db_folder, f"{db_name}_embed_format_{date_str}.json")

    return summary_file, json_schema_file


def table_schema_to_json(conn, output_file):
    """Extracts table schema with PKs and FKs."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()[0]

            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]

        schema_data = []

        for table in tables:
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW COLUMNS FROM `{table}`;")
                raw_columns = [row[0] for row in cursor.fetchall()]
                all_columns = [f"{table}.{col}" for col in raw_columns]

                cursor.execute("""
                    SELECT column_name FROM information_schema.COLUMNS
                    WHERE table_schema = %s AND table_name = %s AND column_key = 'PRI';
                """, (db_name, table))
                pk_columns = [f"{table}.{row[0]}" for row in cursor.fetchall()]

                cursor.execute("""
                    SELECT column_name FROM information_schema.KEY_COLUMN_USAGE
                    WHERE table_schema = %s AND table_name = %s AND referenced_table_name IS NOT NULL;
                """, (db_name, table))
                fk_columns = [f"{table}.{row[0]}" for row in cursor.fetchall()]

                schema_data.append({
                    "table": table,
                    "columns": all_columns,
                    "primary_keys": pk_columns,
                    "foreign_keys": fk_columns
                })

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(schema_data, f, indent=2, ensure_ascii=False)
        return output_file

    except Error as e:
        print(f"Error in table_schema_to_json: {e}")
        return None


def embed_schema_json_structure(conn, output_path):
    """Creates enriched table structure JSON for embedding."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE();")
            _ = cursor.fetchone()

            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]

        schema_data = []

        for table in tables:
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW COLUMNS FROM `{table}`;")
                column_rows = cursor.fetchall()
                columns = [f"{table}.{row[0]}" for row in column_rows]
                column_descriptions = {col: "" for col in columns}

                schema_data.append({
                    "table": table,
                    "description": "",
                    "columns": columns,
                    "column_descriptions": column_descriptions,
                    "tags": [],
                    "example_queries": [],
                    "category": ""
                })

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(schema_data, f, indent=4, ensure_ascii=False)
        return output_path

    except Error as e:
        print(f"Error in embed_schema_json_structure: {e}")
        return None


def start_mysql_documentation(conn):
    """Runs both schema exports and returns file paths."""
    try:
        if not conn:
            print("No connection provided.")
            return None, None

        with conn.cursor() as cursor:
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()[0]

        summary_path, embed_path = generate_output_paths(db_name)

        # Important: both these functions must use with-cursors internally too
        embed_result = embed_schema_json_structure(conn, embed_path)
        summary_result = table_schema_to_json(conn, summary_path)

        print("Documentation completed.")
        return embed_result, summary_result

    except Error as e:
        print(f"Error: {e}")
        return None, None
