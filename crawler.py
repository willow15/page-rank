# description: web crawling

import urllib
from urlparse import urlparse
from urlparse import urljoin
import sqlite3
from BeautifulSoup import *

# 0. create tables in SQLite
conn = sqlite3.connect("repository.sqlite")
cur = conn.cursor()
cur.executescript('''
    CREATE TABLE IF NOT EXISTS Pages(
        id INTEGER NOT NULL PRIMARY KEY,
        url TEXT UNIQUE,
        html TEXT,
        error TEXT,
        old_rank REAL,
        new_rank REAL
    );
    CREATE TABLE IF NOT EXISTS Links(
        from_id INTEGER,
        to_id INTEGER
    )
''')

count = 0
while True:
    # 1. decide how many pages to retrieve
    if count < 1:
        scount = raw_input("How many pages: ")
        if len(scount) < 1: break
        count = int(scount)

    print "count:", count
    # 2. check to see if we are already in progress
    cur.execute('SELECT id, url FROM Pages WHERE html IS NULL AND error IS NULL ORDER BY RANDOM() LIMIT 1')
    row = cur.fetchone()
    if row is not None:
        fromid = row[0]
        starturl = row[1]
    else:
        starturl = raw_input("Enter web url: ")
        if len(starturl) < 1: starturl = "http://python-data.dr-chuck.net/"
        if starturl.endswith('/'): starturl = starturl[:-1]
        if len(starturl) > 1:
            cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES(?, NULL, 1.0)', (starturl, ))
            conn.commit()
            cur.execute('SELECT id FROM Pages WHERE url = ?', (starturl, ))
            row = cur.fetchone()
            fromid = row[0]
    print "Retrieving:", starturl

    # 3. retrieve page
    try:
        document = urllib.urlopen(starturl)
        if document.getcode() != 200:
            print "Error code: ", document.getcode()
            cur.execute('UPDATE Pages SET error = ? WHERE url = ?', (document.getcode(), starturl))
            conn.commit()
            continue
        if document.info().gettype() != "text/html":
            print "Ignore non text/html page"
            cur.execute('UPDATE Pages SET error = -1 WHERE url = ?', (starturl, ))
            conn.commit()
            continue
        html = document.read()
        soup = BeautifulSoup(html)
    except KeyboardInterrupt:
        print "Program interrupted by user..."
        break
    except:
        print "Unable to retrieve or parse page"
        cur.execute('UPDATE Pages SET error = -1 WHERE url = ?', (starturl, ))
        conn.commit()
        continue
    cur.execute('UPDATE Pages SET html = ? WHERE url = ?', (buffer(html), starturl))
    conn.commit()
    count = count - 1
    print "Retrieved:", len(html), "characters"

    # 4. parse page
    tags = soup('a')
    for tag in tags:
        href = tag.get("href", None)
        if href is None: continue

        # 4.1. resolve relative url like href = "/contact" and non text/html page
        up = urlparse(href)
        if len(up.scheme) < 1:
            href = urljoin(starturl, href)
        ipos = href.find('#')
        if ipos > 1: href = href[:ipos]
        if href.endswith('/'): href = href[:-1]
        if href.endswith(".png") or href.endswith(".jpg") or href.endswith(".gif"): continue
        if len(href) < 1: continue

        # 4.2 update linking relation
        cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES(?, NULL, 1.0)', (href, ))
        conn.commit()
        cur.execute('SELECT id FROM Pages WHERE url = ?', (href, ))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print "Could not retrieve id"
            continue
        cur.execute('INSERT INTO Links(from_id, to_id) VALUES(?, ?)', (fromid, toid))
        conn.commit()

cur.close()
