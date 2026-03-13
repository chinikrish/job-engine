import requests
from bs4 import BeautifulSoup
import sqlite3
from discovery import discover_links


KEYWORDS = [
"recruitment",
"notification",
"vacancy",
"apply",
"job",
"posts"
]


def crawl_jobs():

    urls = discover_links()[:20]

    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY,
        title TEXT UNIQUE,
        source TEXT
    )
    """)

    for url in urls:

        try:

            r = requests.get(url, timeout=10)

            soup = BeautifulSoup(r.text,"html.parser")

            for a in soup.find_all("a"):

                text = a.text.strip()

                if len(text) < 25:
                    continue

                lower = text.lower()

                if any(k in lower for k in KEYWORDS):

                    try:

                        cur.execute(
                            "INSERT INTO jobs(title,source) VALUES(?,?)",
                            (text,url)
                        )

                    except:
                        pass

        except:
            pass

    conn.commit()
    conn.close()