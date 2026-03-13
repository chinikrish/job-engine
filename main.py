from fastapi import FastAPI, Query, HTTPException
import sqlite3
from crawler import crawl_jobs
from typing import Optional, List
from datetime import datetime

app = FastAPI(title="Job Crawler API", description="API for accessing crawled job data")

@app.get("/")
def home():
    return {
        "status": "job engine running",
        "endpoints": {
            "/jobs": "Get latest jobs",
            "/jobs/search": "Search jobs with filters",
            "/jobs/{job_id}": "Get specific job details",
            "/crawl": "Trigger manual crawl"
        }
    }

@app.get("/crawl")
def trigger_crawl():
    """Manually trigger job crawling"""
    try:
        crawl_jobs()
        return {"status": "success", "message": "Crawling completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs")
def get_jobs(limit: int = Query(50, ge=1, le=200)):
    """
    Get latest jobs with optional limit
    """
    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()
    
    cur.execute("""
    SELECT id, title, company, location, qualification, salary, 
           age_limit, experience, skills_required, vacancies, 
           job_type, posted_date, last_date, application_link, source
    FROM jobs
    ORDER BY id DESC
    LIMIT ?
    """, (limit,))
    
    rows = cur.fetchall()
    conn.close()
    
    jobs = []
    for r in rows:
        jobs.append({
            "id": r[0],
            "title": r[1],
            "company": r[2],
            "location": r[3],
            "qualification": r[4],
            "salary": r[5],
            "age_limit": r[6],
            "experience": r[7],
            "skills_required": r[8],
            "vacancies": r[9],
            "job_type": r[10],
            "posted_date": r[11],
            "last_date": r[12],
            "application_link": r[13],
            "source": r[14]
        })
    
    return {"total": len(jobs), "jobs": jobs}

@app.get("/jobs/search")
def search_jobs(
    keyword: Optional[str] = Query(None, description="Search in title"),
    location: Optional[str] = Query(None, description="Filter by location"),
    qualification: Optional[str] = Query(None, description="Filter by qualification"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    min_salary: Optional[int] = Query(None, description="Minimum salary"),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Search jobs with various filters
    """
    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()
    
    query = """
    SELECT id, title, company, location, qualification, salary, 
           age_limit, experience, skills_required, vacancies, 
           job_type, posted_date, last_date, application_link, source
    FROM jobs
    WHERE 1=1
    """
    params = []
    
    if keyword:
        query += " AND (title LIKE ? OR company LIKE ? OR skills_required LIKE ?)"
        params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
    
    if location:
        query += " AND location LIKE ?"
        params.append(f"%{location}%")
    
    if qualification:
        query += " AND qualification LIKE ?"
        params.append(f"%{qualification}%")
    
    if job_type:
        query += " AND job_type LIKE ?"
        params.append(f"%{job_type}%")
    
    # Note: Salary filtering is approximate since salary is stored as text
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    
    jobs = []
    for r in rows:
        jobs.append({
            "id": r[0],
            "title": r[1],
            "company": r[2],
            "location": r[3],
            "qualification": r[4],
            "salary": r[5],
            "age_limit": r[6],
            "experience": r[7],
            "skills_required": r[8],
            "vacancies": r[9],
            "job_type": r[10],
            "posted_date": r[11],
            "last_date": r[12],
            "application_link": r[13],
            "source": r[14]
        })
    
    return {"total": len(jobs), "jobs": jobs}

@app.get("/jobs/{job_id}")
def get_job_details(job_id: int):
    """
    Get detailed information for a specific job
    """
    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()
    
    cur.execute("""
    SELECT id, title, company, location, qualification, salary, 
           age_limit, experience, skills_required, vacancies, 
           job_type, posted_date, last_date, application_link, source, crawled_date
    FROM jobs
    WHERE id = ?
    """, (job_id,))
    
    row = cur.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "id": row[0],
        "title": row[1],
        "company": row[2],
        "location": row[3],
        "qualification": row[4],
        "salary": row[5],
        "age_limit": row[6],
        "experience": row[7],
        "skills_required": row[8],
        "vacancies": row[9],
        "job_type": row[10],
        "posted_date": row[11],
        "last_date": row[12],
        "application_link": row[13],
        "source": row[14],
        "crawled_date": row[15]
    }

@app.get("/stats")
def get_stats():
    """
    Get statistics about crawled jobs
    """
    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM jobs")
    total_jobs = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT company) FROM jobs")
    total_companies = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT location) FROM jobs")
    total_locations = cur.fetchone()[0]
    
    cur.execute("""
    SELECT job_type, COUNT(*) 
    FROM jobs 
    WHERE job_type != 'Full-time' 
    GROUP BY job_type
    """)
    job_type_counts = dict(cur.fetchall())
    
    cur.execute("""
    SELECT date(crawled_date), COUNT(*) 
    FROM jobs 
    GROUP BY date(crawled_date)
    ORDER BY date(crawled_date) DESC
    LIMIT 7
    """)
    daily_counts = dict(cur.fetchall())
    
    conn.close()
    
    return {
        "total_jobs": total_jobs,
        "total_companies": total_companies,
        "total_locations": total_locations,
        "job_types": job_type_counts,
        "recent_crawls": daily_counts
    }
