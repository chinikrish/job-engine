from googlesearch import search

# Google search dorks
DORKS = [

'site:gov.in recruitment notification',
'site:gov.in vacancy notification',
'site:gov.in filetype:pdf recruitment',
'"APPSC recruitment notification"',
'"Andhra Pradesh government jobs notification"'

]

# trusted job sources
STATIC_SOURCES = [

"https://freejobalert.com/",
"https://www.mysarkarinaukri.com/",
"https://sarkari-naukri.in/",
"https://indgovtjobs.in/",
"https://freshersworld.com/",
"https://psc.ap.gov.in/",
"https://ap.gov.in/",
"https://employmentnews.gov.in/",
"https://apslprb.ap.gov.in/",
"https://apsrtc.ap.gov.in/"
"https://www.freejobalert.com/ap-government-jobs/",
"https://www.freejobalert.com/ssc-jobs/",
"https://www.freejobalert.com/railway-jobs/",
"https://www.indgovtjobs.in",
]


def discover_links():

    links = []

    # add trusted sources
    links.extend(STATIC_SOURCES)

    # discover new links from google
    for query in DORKS:

        try:

            results = search(query, num_results=5)

            for r in results:
                links.append(r)

        except:
            pass

    # remove duplicates
    links = list(set(links))

    return links
