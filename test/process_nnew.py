from sqlalchemy import *
import csv
from bs4 import BeautifulSoup
import requests, re

blacklist = ['https://twitter.com/share',
             'https://www.addtoany.com/share_save',
             'http://facebook.naturalnews.com',
             'javascript',
             '/cdn-cgi',
             '//www.pinterest.com/pin/',
             'https://translate.google.com/']

def checkAccept(s: str):
    for b in blacklist:
        if s.startswith(b):
            return False
    return True

# Setting up the sql connection to the database
engine = create_engine("mysql+pymysql://vaccine:vaccine@localhost/vaccinedata", pool_pre_ping=True)
conn = engine.connect()

# Reads the table information
metadata = MetaData(conn)
metadata.reflect()

# Getting an 'object' that reflects the articles table in the db
articles = metadata.tables["articles"]

# select queries the db, pulling the columns named here
s = select([articles.c.id,articles.c.link,articles.c.raw_content])

#execute the query
result =conn.execute(s)
print("Done querying")

count = 0
#open a file to save our results
with open("links.csv","w") as f:
    writer = csv.writer(f)
    # this is a csv file with the following header
    writer.writerow(["id","article","link","external","bare","domain"])

    # for each result from the db
    for row in result:
        id = row['id']
        link = row['link']
        file = row["raw_content"]

        # create a scraper
        bs = BeautifulSoup(file, 'html.parser')

        # Look for all articles in the div with the id 'Article' (there should be only)
        for article in bs.find_all('div', {'id': 'Article'}):

            # Find all 'a' tags in the article
            for a in article.find_all('a'):
                # Get the 'href' attribute
                href:str = a.get('href')
                count = count+1
                # If it doesn't contain naturalnews.com, write it to the file
                if href!=None and len(href) > 0 and checkAccept(href):
                    external = 1
                    bare = 1
                    if ("naturalnews.com" in href):
                        external = 0
                    m = re.match(r'\s*[Hh]+ttps?://([^/]+)/?(.*)',href)
                    if m is not None:
                        domain = m.group(1)
                        if m.group(2):
                            bare = 0

                        # get just the domain
                        domain = ".".join(domain.split(".")[-2:])


                    else:
                        print("no match in {} ({}) - [{}]".format(id,link,href))

                    writer.writerow([id,link,href,external,bare,domain])
                    if count % 1000 == 0:
                        print("Processed ",str(count))