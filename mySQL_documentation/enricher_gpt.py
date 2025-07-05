from datetime import datetime
import os
import json
from openai import OpenAI
from dotenv import load_dotenv

# === Load .env securely ===
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# === Chat with GPT-4 ===
def ask_chat(user_prompt):
    # System instruction for enriching school database schemas
    system_instruction = """
    You are an expert in enriching educational database schemas to improve their semantic embeddings for LLM-powered school management applications.

    Your task is to complete the following fields in each JSON schema object:
    - "description"
    - "column_descriptions"
    - "tags"
    - "category"

    Guidelines:

    1. **"description"** should explain the purpose of the table in the context of a digital school management platform. Consider common workflows used by teachers, administrators, students, and parents, such as:
       - Student enrollment and academic tracking
       - Teacher performance and class assignments
       - Parent communication and involvement
       - Academic assessment and grading
       - Attendance and behavior monitoring
       - Extracurricular activity planning
       - Administrative operations and compliance

       Do **not** mention any country, region, or local government. Focus on the table's role in a school platform.

    2. **"column_descriptions"** must clearly describe what each column represents. Use practical language familiar to school staff and platform users. Emphasize how the data connects to school processes such as courses, users, grades, and attendance.

    3. **"tags"** should include 8–12 keywords that users of a school platform might search for. Cover aspects like:
       - **Administrative**: ["enrollment", "academic year", "student records"]
       - **Academic**: ["subjects", "grades", "evaluations", "curriculum"]
       - **Operational**: ["attendance", "behavior", "communication", "events"]
       - **Reporting**: ["performance tracking", "report cards", "progress monitoring"]

    4. **"category"** should be a concise phrase that describes the main purpose of the table. Examples include:
       - "student information"
       - "attendance tracking"
       - "academic records"
       - "assessment data"
       - "teacher management"
       - "parent communication"
       - "school operations"
       - "extracurricular activities"

    5. **Contextual Considerations**:
       - Do not refer to specific countries, ministries, or regions.
       - Use school-related terminology only (e.g., academic term, user ID, class group).
       - If the data includes calendar fields, assume mixed support for Gregorian and Hijri calendars without mentioning them.

    6. **Data Relationships**: When describing columns, consider how they might connect to other tables:
       - Student IDs linking to grades, attendance, and behavior
       - Teacher/user IDs linking to classes and subjects
       - Academic terms/years influencing time-based records
       - Guardian IDs linking to students

    7. Leave the "example_queries" field empty.

    **Output Requirements**:
    - Return ONLY a valid JSON object
    - Keep the exact same structure as the input
    - Fill only the specified fields: "description", "column_descriptions", "tags", "category"
    - Use clear, professional, and platform-appropriate language
    - Avoid unnecessary repetition
    - Avoid any mention of regions, countries, or ministries

    **Quality Standards**:
    - "description": 2–4 sentences on the table’s purpose and platform usage
    - "column_descriptions": 1–2 clear lines per column
    - "tags": 8–12 relevant, searchable keywords
    - "category": one concise classification phrase
    """

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.0  # Make it deterministic and precise
    )
    return response.choices[0].message.content.strip()


# === Generator to yield individual JSON docs ===
def get_each_doc_from_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if isinstance(data, list):
                for doc in data:
                    yield doc
            elif isinstance(data, dict):
                yield data
            else:
                yield data
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON Decode Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


# === Main Enrichment Function ===
def enrich_the_DB_schema(json_embed_file):
    filled_documents = []
    for i, doc in enumerate(get_each_doc_from_json(json_embed_file), 1):
        user_prompt = f"Please fill the empty fields using meaningful descriptions:\n{json.dumps(doc, indent=2)}"
        raw_response = ask_chat(user_prompt)
        try:
            filled_doc = json.loads(raw_response)
            filled_documents.append(filled_doc)
            print(filled_doc)
            print(f"✅ Filled: {filled_doc['table']}")
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding response for table {doc.get('table', 'unknown')}: {e}")
            continue

    # ✅ Use timestamp
    date_str = datetime.today().strftime("%Y-%m-%d")

    # ✅ Save in same folder as json_embed_file
    output_dir = os.path.dirname(json_embed_file)
    output_file = os.path.join(output_dir, f"enriched_schema_{date_str}.json")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filled_documents, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Successfully saved {len(filled_documents)} documents to {output_file}")
