from fastapi import FastAPI
import sqlite3
from crawler import crawl_jobs

app = FastAPI()


@app.get("/")
def home():

    return {"status":"job engine running"}


@app.get("/jobs")

def get_jobs():

    crawl_jobs()

    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()

    cur.execute("""
    SELECT title,source
    FROM jobs
    ORDER BY id DESC
    LIMIT 30
    """)

    rows = cur.fetchall()

    conn.close()

    jobs = []

    for r in rows:

        jobs.append({
            "title": r[0],
            "source": r[1]
        })

    return {"jobs":jobs}
