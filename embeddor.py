import json
from tqdm import tqdm
from langchain_openai import OpenAIEmbeddings
from config import OPENAI_API_KEY, EMBEDDING_MODEL

embedder = OpenAIEmbeddings(model=EMBEDDING_MODEL, openai_api_key=OPENAI_API_KEY)


def generate_rich_schema_text(schema: dict) -> str:
    text = f"Table: {schema['table']}\n"
    text += f"Description: {schema.get('description', '')}\n"
    text += f"Category: {schema.get('category', '')}\n"
    tags = schema.get("tags", [])
    if tags:
        text += f"Tags: {', '.join(tags)}\n"
    text += "Columns and Descriptions:\n"
    for col in schema.get("columns", []):
        desc = schema.get("column_descriptions", {}).get(col, "")
        text += f"- {col}: {desc}\n"
    return text


def embed_and_save_with_tags(input_path, output_path):
    with open(input_path, "r", encoding="utf-8") as f:
        schema_data = json.load(f)

    embedded_output = []

    for table in tqdm(schema_data, desc="Embedding tables"):
        content = generate_rich_schema_text(table)
        embedding = embedder.embed_query(content)

        embedded_output.append({
            "table": table["table"],
            "embedding": embedding,
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(embedded_output, f, indent=2)

    print(f"✅ Saved embedded file with tags/categories to: {output_path}")


if __name__ == "__main__":
    input_path = r"C:\Users\wajee\PycharmProjects\MySQLinsight\int-elearning\enriched_schema_2025-06-23.json"  # ← your input file
    output_path = r"C:\Users\wajee\PycharmProjects\MySQLinsight\int-elearning\int-elearning_embeddings_2025-06-23.json"  # ← your desired output file

    embed_and_save_with_tags(input_path, output_path)
