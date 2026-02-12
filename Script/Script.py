# =========================
# CONFIG + HELPERS
# =========================
import re
import time
import requests
import pandas as pd
from datetime import datetime

# ---- KEYS ----
APP_ID = "123"
APP_KEY = "123"

# ---- PROJECT SETTINGS ----
COUNTRY = "za"                 # South Africa
RESULTS_PER_PAGE = 50          # keep <= 50
PAGES_TO_FETCH = 3             # start small to avoid rate limits
SLEEP_SECONDS = 1              # polite delay

SEARCH_TERMS = [
    "data analyst",
    "data scientist",
    "power bi",
    "sql analyst",
    "hr analyst",
]

SKILLS = [
    "python","sql","power bi","excel","tableau",
    "pandas","numpy",
    "spark","pyspark","databricks","snowflake",
    "aws","azure","gcp",
    "sas","r",
    "etl","data engineering",
    "machine learning","statistics",
    "git","docker"
]

PATTERNS = {s: re.compile(rf"\b{re.escape(s)}\b", re.IGNORECASE) for s in SKILLS}

def extract_skills(text: str):
    if not text:
        return []
    lower = text.lower()
    found = []
    for s, p in PATTERNS.items():
        if s in lower and p.search(text):
            found.append(s)
    return found

def fetch_page(term: str, page: int):
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}"
    params = {
        "app_id": APP_ID,
        "app-key": APP_KEY,
        "results_per_page": RESULTS_PER_PAGE,
        "what": term
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# =========================
# SCRAPING THE DATA
# =========================
rows = []
scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
for term in SEARCH_TERMS:
   print(f"\n=== Searching: {term} ===")
   for page in range(1, PAGES_TO_FETCH + 1):
       print(f"Fetching page {page}/{PAGES_TO_FETCH} ...")
       try:
           data = fetch_page(term, page)
       except requests.HTTPError as e:
           print(f" Error fetching page {page} for '{term}'")
           print(e)
           break
       results = data.get("results", [])
       if not results:
           print("No results, stopping this term.")
           break
       for item in results:
           desc = item.get("description") or ""
           skills_found = extract_skills(desc)
           rows.append({
               "scraped_at": scraped_at,
               "search_term": term,
               "job_id": item.get("id"),
               "title": item.get("title"),
               "company": (item.get("company") or {}).get("display_name"),
               "location": (item.get("location") or {}).get("display_name"),
               "category": (item.get("category") or {}).get("label"),
               "contract_type": item.get("contract_type"),
               "created": item.get("created"),
               "salary_min": item.get("salary_min"),
               "salary_max": item.get("salary_max"),
               "skills": ", ".join(skills_found),
               "skills_count": len(skills_found),
               "redirect_url": item.get("redirect_url")
           })
       time.sleep(SLEEP_SECONDS)
df = pd.DataFrame(rows)
if df.empty:
   raise RuntimeError("No rows collected. Check API keys or limits.")
df = df.drop_duplicates(subset=["job_id"])
print(f"\n Collected {len(df):,} unique jobs")
display(df.head(10))


# =========================
# CREATING THE JOBS TABLE
# =========================
# Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)
# Save as Delta table
spark_df.write \
   .mode("overwrite") \
   .format("delta") \
   .saveAsTable("jobs_adzuna")
print("Delta table created: jobs_adzuna")
display(spark.table("jobs_adzuna").limit(10))

from pyspark.sql import functions as F

# Load the jobs table from Spark / Delta
jobs_df = spark.table("jobs_adzuna")

# Transform the skills column into one row per skill
skills_df = (
    jobs_df
    # Split the comma-separated skills string into an array,
    # then explode it so each skill becomes its own row
    .withColumn("skill", F.explode(F.split(F.col("skills"), ", ")))
    
    # Remove null or empty skill values
    .filter(F.col("skill").isNotNull() & (F.col("skill") != ""))
    
    # Select only the relevant job and skill columns
    .select(
        "job_id",
        "title",
        "company",
        "location",
        "created",
        "search_term",
        "skill"
    )
)

# Write the transformed data as a Delta table,
# overwriting the table if it already exists
skills_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("jobs_adzuna_skills")

# Confirmation message
print("Delta table created: jobs_adzuna_skills")

# Display a sample of 20 rows to verify the output
display(skills_df.limit(20))



