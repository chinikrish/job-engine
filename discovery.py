from googlesearch import search

queries = [

"site:gov.in recruitment notification",
"site:gov.in vacancy notification",
"Andhra Pradesh government jobs notification",
"APPSC recruitment notification",
"government job notification pdf"

]

def discover_links():

    links = []

    for q in queries:

        try:

            results = search(q, num_results=5)

            for r in results:
                links.append(r)

        except:
            pass

    return list(set(links))