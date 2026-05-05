# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install pymongo --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("MONGO_URI", "", "MongoDB Connection URI")
dbutils.widgets.text("UC_CATALOG", "", "Unity Catalog Name")
dbutils.widgets.text("UC_SCHEMA", "", "Unity Catalog Schema")
dbutils.widgets.text("SKIP_DATABASES", "admin,local,config", "Databases to Skip (comma-separated)")
dbutils.widgets.text("SKIP_COLLECTIONS_PREFIX", "system.", "Collection Prefixes to Skip (comma-separated)")
dbutils.widgets.text("INCLUDE_DATABASES", "", "Databases to Include (comma-separated, empty = all)")
dbutils.widgets.text("WRITE_MODE", "overwrite", "Write Mode (overwrite / append)")

# COMMAND ----------

MONGO_URI = dbutils.widgets.get("MONGO_URI")
UC_CATALOG = dbutils.widgets.get("UC_CATALOG")
UC_SCHEMA = dbutils.widgets.get("UC_SCHEMA")
WRITE_MODE = dbutils.widgets.get("WRITE_MODE") or "overwrite"

SKIP_DATABASES = {
    db.strip() for db in dbutils.widgets.get("SKIP_DATABASES").split(",") if db.strip()
}
SKIP_COLLECTIONS_PREFIX = {
    p.strip() for p in dbutils.widgets.get("SKIP_COLLECTIONS_PREFIX").split(",") if p.strip()
}
INCLUDE_DATABASES = [
    db.strip() for db in dbutils.widgets.get("INCLUDE_DATABASES").split(",") if db.strip()
]

assert MONGO_URI, "MONGO_URI is required"
assert UC_CATALOG, "UC_CATALOG is required"
assert UC_SCHEMA, "UC_SCHEMA is required"

print(f"Catalog:    {UC_CATALOG}")
print(f"Schema:     {UC_SCHEMA}")
print(f"Write mode: {WRITE_MODE}")
print(f"Skip DBs:   {SKIP_DATABASES}")
print(f"Skip prefixes: {SKIP_COLLECTIONS_PREFIX}")
print(f"Include DBs:   {INCLUDE_DATABASES or '(all)'}")

# COMMAND ----------

from pymongo import MongoClient

client = MongoClient(MONGO_URI)
client.admin.command("ping")
print("Connected to MongoDB")

# COMMAND ----------

all_databases = client.list_database_names()

if INCLUDE_DATABASES:
    databases = [db for db in INCLUDE_DATABASES if db in all_databases]
else:
    databases = [db for db in all_databases if db not in SKIP_DATABASES]

print(f"Databases to process: {databases}")

# COMMAND ----------

import re
import pandas as pd
from pyspark.sql import functions as F

def sanitize_table_name(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized.lower()

def should_skip_collection(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in SKIP_COLLECTIONS_PREFIX)

def flatten_mongo_doc(doc: dict) -> dict:
    """Stringify nested dicts/lists and ObjectIds so Pandas can handle them."""
    flat = {}
    for key, value in doc.items():
        if isinstance(value, (dict, list)):
            flat[key] = str(value)
        else:
            flat[key] = str(value) if not isinstance(value, (int, float, bool, type(None))) else value
    return flat

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

summary = []

for db_name in databases:
    db = client[db_name]
    collections = [c for c in db.list_collection_names() if not should_skip_collection(c)]
    print(f"\n{'='*60}")
    print(f"Database: {db_name} — {len(collections)} collection(s)")
    print(f"{'='*60}")

    for coll_name in collections:
        table_name = sanitize_table_name(f"{db_name}__{coll_name}")
        full_table = f"{UC_CATALOG}.{UC_SCHEMA}.{table_name}"

        try:
            docs = list(db[coll_name].find())
            if not docs:
                print(f"  SKIP {coll_name} (empty)")
                summary.append((db_name, coll_name, full_table, 0, "skipped (empty)"))
                continue

            flat_docs = [flatten_mongo_doc(doc) for doc in docs]
            pdf = pd.DataFrame(flat_docs)

            if "_id" in pdf.columns:
                pdf["_id"] = pdf["_id"].astype(str)

            sdf = spark.createDataFrame(pdf)
            sdf.write.mode(WRITE_MODE).saveAsTable(full_table)

            print(f"  OK   {coll_name} → {full_table} ({len(docs)} docs)")
            summary.append((db_name, coll_name, full_table, len(docs), "ok"))

        except Exception as e:
            print(f"  FAIL {coll_name} → {e}")
            summary.append((db_name, coll_name, full_table, 0, str(e)))

# COMMAND ----------

summary_df = spark.createDataFrame(
    summary, ["database", "collection", "table", "doc_count", "status"]
)
display(summary_df)

# COMMAND ----------

client.close()
print("Done")

