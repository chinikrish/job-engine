import requests
from bs4 import BeautifulSoup
import sqlite3
from discovery import discover_links


def crawl():

    urls = discover_links()

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

            r = requests.get(url, timeout=8)

            soup = BeautifulSoup(r.text,"html.parser")

            title = soup.title.text.strip()

            if "Recruitment" in title or "Vacancy" in title or "Notification" in title:

                try:

                    cur.execute(
                        "INSERT INTO jobs(title,source) VALUES(?,?)",
                        (title,url)
                    )

                except:
                    pass

        except:
            pass

    conn.commit()
    conn.close()