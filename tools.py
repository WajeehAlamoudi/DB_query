from langchain_openai import OpenAIEmbeddings
from config import *
import json
import numpy as np

embedder = OpenAIEmbeddings(model=EMBEDDING_MODEL, openai_api_key=OPENAI_API_KEY)


def embed_text(text: str) -> list:
    """Returns the embedding vector of the input text."""
    return embedder.embed_query(text)


def load_schema_embeddings(path=schema_embddings_path):
    with open(path, "r", encoding="utf-8") as f:
        schema_data = json.load(f)
    for schema in schema_data:
        schema["embedding"] = np.array(schema["embedding"])  # ensure numpy format
    return schema_data


def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


def find_related_tables(user_embedding, top_n=3):
    schema_data = load_schema_embeddings()
    similarities = []

    for table in schema_data:
        sim = cosine_similarity(user_embedding, table["embedding"])
        similarities.append((sim, table))

    # Sort by similarity
    similarities.sort(reverse=True, key=lambda x: x[0])

    # Return only the table names and similarity score (optional)
    return [(table["table"], score) for score, table in similarities[:top_n]]


def retrieve_tables_context(table_names, file_path=schema_tabels_path):
    """
    Given a list of table names and a full schema file path,
    returns full schema entries (with description, columns, etc.)
    """
    with open(file_path, "r", encoding="utf-8") as f:
        all_schemas = json.load(f)

    matched_tables = []
    for table in all_schemas:
        if table["table"] in table_names:
            matched_tables.append(table)

    return matched_tables


def build_user_prompt_requesting_sql(user_input):
    user_input_vec = embed_text(user_input)

    # Step 1: Get table names only
    top_matches = find_related_tables(user_input_vec, 3)
    table_names = [name for name, _ in top_matches]

    # Step 2: Load related schema info
    related_tables = retrieve_tables_context(table_names)

    # Step 3: Build SQL prompt
    return (
        f"You are an expert SQL assistant.\n"
        f"Based on the request: \"{user_input}\"\n"
        f"Use the following table schema JSON to answer with ONLY a valid SQL query (MySQL dialect):\n\n"
        f"{json.dumps(related_tables, indent=2)}"
    )


def getting_sql_code():
    pass


from mySQLdb_connection import MySQLClient


def get_embed_path(connection, base_dir=r"C:\Users\wajee\PycharmProjects\MySQLinsight"):
    """
    Given a live MySQL connection, find the database name,
    look for a folder named after it inside `base_dir`, and
    return the path to the first file containing 'embedding' in its name.

    Args:
        connection: mysql.connector connection object
        base_dir (str): Root directory where embedding folders are stored

    Returns:
        str: Full path to the embedding file
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        db_name = cursor.fetchone()[0]
        cursor.close()
    except Exception as e:
        raise RuntimeError(f"❌ Failed to fetch DB name: {e}")

    db_folder = os.path.join(base_dir, db_name)
    if not os.path.isdir(db_folder):
        raise FileNotFoundError(f"❌ Folder not found for DB: {db_folder}")

    for file in os.listdir(db_folder):
        if "embedding" in file.lower():
            file_path = os.path.join(db_folder, file)
            print(f"✅ Found embedding file: {file_path}")
            return file_path

    raise FileNotFoundError(f"❌ No embedding file found in {db_folder}")
