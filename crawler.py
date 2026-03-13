import requests
from bs4 import BeautifulSoup
import sqlite3
from discovery import discover_links
import re
from datetime import datetime
import json

KEYWORDS = [
    "recruitment", "notification", "vacancy", "apply", "job", "posts",
    "advertisement", "career", "employment", "position", "opening"
]

def extract_company_name(soup, url):
    """Extract company/organization name from the page"""
    # Try common patterns
    patterns = [
        ('meta[name="og:site_name"]', 'content'),
        ('meta[property="og:site_name"]', 'content'),
        ('meta[name="twitter:site"]', 'content'),
        ('title', None),
        ('.company-name', None),
        ('#company-name', None),
        ('.organization-name', None),
        ('.org-name', None)
    ]
    
    for selector, attr in patterns:
        if selector.startswith('meta'):
            element = soup.select_one(selector)
            if element and attr:
                value = element.get(attr, '')
                if value:
                    return value.strip()
        else:
            element = soup.select_one(selector)
            if element:
                text = element.text.strip()
                if text and len(text) < 100:
                    return text
    
    # Extract from URL
    url_parts = url.split('/')
    for part in url_parts:
        if 'gov' in part or 'recruitment' in part or 'jobs' in part:
            return part.replace('www.', '').replace('.com', '').replace('.in', '').replace('.org', '')
    
    return "Unknown Organization"

def extract_location(soup, text):
    """Extract location information"""
    location_patterns = [
        r'location[:\s]+([^\n]+)',
        r'place of posting[:\s]+([^\n]+)',
        r'posting place[:\s]+([^\n]+)',
        r'venue[:\s]+([^\n]+)',
        r'job location[:\s]+([^\n]+)',
        r'work location[:\s]+([^\n]+)',
        r'based at[:\s]+([^\n]+)',
        r'(?:all )?india',
        r'andhra pradesh',
        r'telangana',
        r'hyderabad',
        r'vijayawada',
        r'visakhapatnam',
        r'tirupati',
        r'guntur'
    ]
    
    # Search in text
    for pattern in location_patterns:
        match = re.search(pattern, text.lower())
        if match:
            location = match.group(0) if pattern in ['india', 'andhra pradesh', 'telangana'] else match.group(1)
            return location.capitalize()
    
    # Search in common HTML elements
    location_selectors = ['.location', '#location', '.job-location', '.place', '.venue', '[itemprop="jobLocation"]']
    for selector in location_selectors:
        element = soup.select_one(selector)
        if element:
            return element.text.strip()
    
    return "Not Specified"

def extract_qualification(soup, text):
    """Extract educational qualification"""
    qual_patterns = [
        r'qualification[:\s]+([^\n]+)',
        r'educational qualification[:\s]+([^\n]+)',
        r'education[:\s]+([^\n]+)',
        r'essential qualification[:\s]+([^\n]+)',
        r'academic qualification[:\s]+([^\n]+)',
        r'degree[:\s]+([^\n]+)',
        r'graduate',
        r'post[-\s]graduate',
        r'diploma',
        r'b\.?tech',
        r'b\.?e',
        r'm\.?tech',
        r'm\.?sc',
        r'm\.?a',
        r'b\.?a',
        r'b\.?com',
        r'm\.?com',
        r'bachelor',
        r'master',
        r'phd',
        r'mbbs',
        r'bds',
        r'md',
        r'ms',
        r'10th',
        r'12th',
        r'intermediate'
    ]
    
    qualifications_found = []
    
    for pattern in qual_patterns:
        if pattern.startswith(r''):  # For regex patterns
            match = re.search(pattern, text.lower())
            if match:
                qual = match.group(1) if len(match.groups()) > 0 else match.group(0)
                qualifications_found.append(qual)
        else:  # For simple text patterns
            if pattern in text.lower():
                # Try to capture the full qualification sentence
                lines = text.lower().split('\n')
                for line in lines:
                    if pattern in line and len(line) < 200:
                        qualifications_found.append(line.strip())
    
    # Look for qualification section
    qual_sections = soup.find_all(['h2', 'h3', 'h4', 'strong', 'b'], string=re.compile(r'qualification|education|eligibility', re.I))
    for section in qual_sections:
        next_elem = section.find_next(['p', 'div', 'ul', 'li'])
        if next_elem:
            qualifications_found.append(next_elem.text.strip())
    
    if qualifications_found:
        return ' | '.join(list(set(qualifications_found))[:3])
    
    return "See Notification"

def extract_salary(soup, text):
    """Extract salary/remuneration"""
    salary_patterns = [
        r'salary[:\s]+([^\n]+)',
        r'pay scale[:\s]+([^\n]+)',
        r'pay band[:\s]+([^\n]+)',
        r'remuneration[:\s]+([^\n]+)',
        r'stipend[:\s]+([^\n]+)',
        r'emoluments[:\s]+([^\n]+)',
        r'compensation[:\s]+([^\n]+)',
        r'basic pay[:\s]+([^\n]+)',
        r'gross pay[:\s]+([^\n]+)',
        r'(?:rs\.?|inr)\s*[\d,]+\s*(?:to|-)?\s*(?:rs\.?|inr)?\s*[\d,]+',
        r'(?:rs\.?|inr)\s*[\d,]+',
        r'\$\s*[\d,]+'
    ]
    
    for pattern in salary_patterns:
        match = re.search(pattern, text.lower(), re.I)
        if match:
            if pattern.startswith(r'('):  # Regex with capture groups
                return match.group(1)
            return match.group(0)
    
    # Look for salary in tables
    tables = soup.find_all('table')
    for table in tables:
        if re.search(r'salary|pay|stipend', table.text, re.I):
            return table.text.strip()[:200]
    
    return "Not Disclosed"

def extract_age_limit(soup, text):
    """Extract age limit"""
    age_patterns = [
        r'age limit[:\s]+([^\n]+)',
        r'age[:\s]+([^\n]+)',
        r'maximum age[:\s]+([^\n]+)',
        r'minimum age[:\s]+([^\n]+)',
        r'age as on[:\s]+([^\n]+)',
        r'\d+\s*(?:to|-)\s*\d+\s*years?',
        r'\d+\s*years?'
    ]
    
    for pattern in age_patterns:
        match = re.search(pattern, text.lower(), re.I)
        if match:
            if pattern.startswith(r'('):  # Regex with capture groups
                return match.group(1)
            return match.group(0)
    
    return "As per notification"

def extract_experience(soup, text):
    """Extract experience requirements"""
    exp_patterns = [
        r'experience[:\s]+([^\n]+)',
        r'work experience[:\s]+([^\n]+)',
        r'professional experience[:\s]+([^\n]+)',
        r'relevant experience[:\s]+([^\n]+)',
        r'years? of experience',
        r'experienced',
        r'fresher',
        r'entry level'
    ]
    
    for pattern in exp_patterns:
        if pattern.startswith(r'('):  # Regex patterns
            match = re.search(pattern, text.lower(), re.I)
            if match:
                return match.group(1)
        else:  # Simple text patterns
            if pattern in text.lower():
                lines = text.lower().split('\n')
                for line in lines:
                    if pattern in line and len(line) < 150:
                        return line.strip()
    
    return "Not specified"

def extract_skills(soup, text):
    """Extract skills required"""
    skills = []
    
    # Common skill indicators
    skill_indicators = [
        'skills required', 'required skills', 'skills needed',
        'desired skills', 'key skills', 'technical skills',
        'proficiency in', 'knowledge of', 'familiarity with'
    ]
    
    for indicator in skill_indicators:
        if indicator in text.lower():
            # Try to extract skills section
            lines = text.lower().split('\n')
            for i, line in enumerate(lines):
                if indicator in line:
                    # Get the line and next few lines
                    for j in range(i, min(i+5, len(lines))):
                        if lines[j].strip():
                            skills.append(lines[j].strip())
                    break
    
    # Look for bullet points with skills
    skill_items = soup.find_all(['li', 'div', 'p'], string=re.compile(r'skill|proficiency|knowledge of', re.I))
    for item in skill_items[:5]:
        skills.append(item.text.strip())
    
    # Common skills to look for
    common_skills = ['python', 'java', 'javascript', 'c++', 'sql', 'excel', 
                     'communication', 'leadership', 'management', 'analysis']
    
    for skill in common_skills:
        if skill in text.lower() and skill not in str(skills).lower():
            skills.append(skill.capitalize())
    
    if skills:
        return ', '.join(list(set(skills))[:10])
    
    return "See notification"

def extract_vacancies(soup, text):
    """Extract number of vacancies"""
    vacancy_patterns = [
        r'vacancies?[:\s]+(\d+)',
        r'posts?[:\s]+(\d+)',
        r'positions?[:\s]+(\d+)',
        r'no\.?\s*of\s*posts?[:\s]+(\d+)',
        r'total\s*posts?[:\s]+(\d+)',
        r'\d+\s*(?:vacancies|posts)'
    ]
    
    for pattern in vacancy_patterns:
        if '(\d+)' in pattern:  # Pattern with number capture
            match = re.search(pattern, text.lower(), re.I)
            if match:
                return int(match.group(1))
        else:
            match = re.search(pattern, text.lower(), re.I)
            if match:
                # Extract number from matched string
                numbers = re.findall(r'\d+', match.group(0))
                if numbers:
                    return int(numbers[0])
    
    return "See notification"

def extract_job_type(soup, text):
    """Extract job type (full-time, part-time, contract, etc.)"""
    job_types = ['full[-\s]time', 'part[-\s]time', 'contract', 'temporary', 
                 'permanent', 'regular', 'adhoc', 'deputation', 'outsourcing']
    
    for jt in job_types:
        if re.search(jt, text.lower()):
            return jt.replace('[-\s]', '-').capitalize()
    
    # Look for job type in common selectors
    type_selectors = ['.job-type', '.employment-type', '.job-nature']
    for selector in type_selectors:
        element = soup.select_one(selector)
        if element:
            return element.text.strip()
    
    return "Full-time"

def extract_posted_date(soup, text):
    """Extract posted date"""
    date_patterns = [
        r'posted date[:\s]+([^\n]+)',
        r'date of posting[:\s]+([^\n]+)',
        r'published date[:\s]+([^\n]+)',
        r'advertisement date[:\s]+([^\n]+)',
        r'release date[:\s]+([^\n]+)'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    
    return datetime.now().strftime("%Y-%m-%d")

def extract_last_date(soup, text):
    """Extract last/application date"""
    date_patterns = [
        r'last date[:\s]+([^\n]+)',
        r'closing date[:\s]+([^\n]+)',
        r'due date[:\s]+([^\n]+)',
        r'deadline[:\s]+([^\n]+)',
        r'apply by[:\s]+([^\n]+)',
        r'last date for receipt of application[:\s]+([^\n]+)'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    
    # Try to find dates in common formats
    date_formats = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
        r'\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}',
        r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4}'
    ]
    
    dates = []
    for df in date_formats:
        found_dates = re.findall(df, text, re.I)
        dates.extend(found_dates)
    
    if dates:
        return dates[-1]  # Usually the last date mentioned is the deadline
    
    return "See notification"

def extract_application_link(soup, url):
    """Extract direct application link"""
    # Look for apply buttons/links
    apply_selectors = [
        'a[href*="apply"]',
        'a[href*="registration"]',
        'a[href*="online"]',
        'a[href*="form"]',
        'a:contains("Apply")',
        'a:contains("Register")',
        'a:contains("Online Application")',
        '.apply-button',
        '#apply-now',
        '.btn-apply'
    ]
    
    for selector in apply_selectors:
        if selector.startswith('a:contains'):
            # Handle contains selector differently
            text_to_find = selector.split('"')[1]
            for a in soup.find_all('a'):
                if text_to_find.lower() in a.text.lower():
                    href = a.get('href', '')
                    if href:
                        if href.startswith('http'):
                            return href
                        elif href.startswith('/'):
                            # Construct full URL
                            base_url = '/'.join(url.split('/')[:3])
                            return base_url + href
        else:
            element = soup.select_one(selector)
            if element and element.get('href'):
                href = element.get('href')
                if href.startswith('http'):
                    return href
                elif href.startswith('/'):
                    base_url = '/'.join(url.split('/')[:3])
                    return base_url + href
    
    # Check for PDF links (often the notification contains application details)
    pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
    for pdf in pdf_links:
        href = pdf.get('href')
        if href:
            if href.startswith('http'):
                return href
            elif href.startswith('/'):
                base_url = '/'.join(url.split('/')[:3])
                return base_url + href
    
    return url  # Return the original URL if no specific apply link found

def crawl_jobs():
    urls = discover_links()[:20]  # Limit to 20 URLs for testing
    
    conn = sqlite3.connect("jobs.db")
    cur = conn.cursor()
    
    # Updated table schema with all required fields
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY,
        title TEXT UNIQUE,
        company TEXT,
        location TEXT,
        qualification TEXT,
        salary TEXT,
        age_limit TEXT,
        experience TEXT,
        skills_required TEXT,
        vacancies TEXT,
        job_type TEXT,
        posted_date TEXT,
        last_date TEXT,
        application_link TEXT,
        source TEXT,
        crawled_date TEXT
    )
    """)
    
    for url in urls:
        try:
            print(f"Crawling: {url}")
            r = requests.get(url, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            
            for a in soup.find_all("a"):
                text = a.text.strip()
                
                if len(text) < 25:
                    continue
                
                lower = text.lower()
                
                if any(k in lower for k in KEYWORDS):
                    try:
                        # Extract all fields
                        title = text
                        company = extract_company_name(soup, url)
                        location = extract_location(soup, lower)
                        qualification = extract_qualification(soup, lower)
                        salary = extract_salary(soup, lower)
                        age_limit = extract_age_limit(soup, lower)
                        experience = extract_experience(soup, lower)
                        skills = extract_skills(soup, lower)
                        vacancies = extract_vacancies(soup, lower)
                        job_type = extract_job_type(soup, lower)
                        posted_date = extract_posted_date(soup, r.text)
                        last_date = extract_last_date(soup, r.text)
                        application_link = extract_application_link(soup, url)
                        source = url
                        crawled_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Insert into database
                        cur.execute("""
                            INSERT OR IGNORE INTO jobs(
                                title, company, location, qualification, salary, 
                                age_limit, experience, skills_required, vacancies, 
                                job_type, posted_date, last_date, application_link, 
                                source, crawled_date
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            title, company, location, qualification, salary,
                            age_limit, experience, skills, vacancies,
                            job_type, posted_date, last_date, application_link,
                            source, crawled_date
                        ))
                        
                        print(f"  Added: {title[:50]}...")
                        
                    except Exception as e:
                        print(f"  Error inserting job: {e}")
                        pass
                        
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            pass
    
    conn.commit()
    conn.close()
    print("Crawling completed!")
