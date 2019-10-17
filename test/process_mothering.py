from sqlalchemy import *
import csv
from bs4 import BeautifulSoup
import requests, re
import sys

blacklist = ['https://twitter.com/share',
             'https://www.addtoany.com/share_save',
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
content = metadata.tables["content"]
threads = metadata.tables["threads"]



# select queries the db, pulling the columns named here
s = select([content.c.id,content.c.link,content.c.raw,threads.c.id,threads.c.link]).where(and_(content.c.thread == threads.c.id,threads.c.domain == "mothering.com"))

#execute the query
result =conn.execute(s)
print("Done querying")

count = 0
#open a file to save our results
with open("mothering_links.csv","w") as f:
    writer = csv.writer(f)
    # this is a csv file with the following header
    writer.writerow(["id","threadid","article","link","external","bare","domain"])

    # for each result from the db
    for row in result:
        id = row[content.c.id]
        threadid = row[threads.c.id]
        link = row[content.c.link]
        file = row["raw"]

        # create a scraper
        bs = BeautifulSoup(file, 'html.parser')

        # Look for all articles in the div with the id 'Article' (there should be only)
        for a in bs.select(":not(.quote_box) a"):

            # Get the 'href' attribute
            href:str = a.get('href')
            count = count+1
            # If it doesn't contain naturalnews.com, write it to the file
            if href!=None and len(href) > 0 and checkAccept(href):
                external = 1
                bare = 1
                if ("mothering.com" in href):
                    external = 0
                m = re.match(r'\s*[Hh]+ttps?://([^/]+)/?(.*)',href)
                if m is not None:
                    domain = m.group(1)
                    if m.group(2):
                        bare = 0

                    # get just the domain
                    domain = ".".join(domain.split(".")[-2:])


                writer.writerow([id,threadid,link,href,external,bare,domain])
                if count % 1000 == 0:
                    print("Processed ",str(count))