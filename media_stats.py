import re
import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from plotnine import ggplot, aes, geom_area, scale_x_datetime

API_URL = "https://lostmediawiki.com/w/api.php"

def extract_content_page_id():
    params = {
        "action" : "query",
        "format" : "json",
        "list"   : "allpages",
        "apfilterredir" : "nonredirects",
        "aplimit" : "500"
    }
    headers = {"User-Agent": "Geoff/1.0"}

    con = sqlite3.connect("lostmediawiki.db")
    cur = con.cursor()

    if (cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='content_pages'").fetchone() is None):
        cur.execute("CREATE TABLE content_pages ( \
            page_id INT PRIMARY KEY \
        );")

    while True:
        req = requests.get(API_URL, headers=headers, params=params)
        res = req.json()
        for page in res["query"]["allpages"]:
            cur.execute("INSERT OR IGNORE INTO content_pages (page_id) VALUES (?)", (page["pageid"],))

        if "continue" not in res.keys():
            break
        params["apcontinue"] = res["continue"]["apcontinue"]

    con.commit()
    con.close()

def get_revisions_for_page_id(page_id, cur):
    params = {
        "action" : "query",
        "format" : "json",
        "prop"   : "revisions",
        "pageids" : str(page_id),
        "rvprop" : "ids|timestamp|comment|user|content",
        "rvslots" : "main",
        "rvlimit" : "50"
    }
    headers = {"User-Agent": "Geoff/1.0"}

    req = requests.get(API_URL, headers=headers, params=params)
    result = req.json()

    for revision in result["query"]["pages"][str(page_id)]["revisions"]:
        username = None if "userhidden" in revision.keys() else revision["user"]
        content = None if "texthidden" in revision["slots"]["main"].keys() else revision["slots"]["main"]["*"]
        comment = None if "commenthidden" in revision.keys() else revision["comment"]
        cur.execute("INSERT OR IGNORE INTO revisions (rev_id, page_id, user, timestamp, content, comment) VALUES \
                    (?, ?, ?, ?, ?, ?)", (revision["revid"], str(page_id), username, revision["timestamp"], content, comment))

def get_revisions_for_all_page_ids():
    con = sqlite3.connect("lostmediawiki.db")
    cur = con.cursor()

    if (cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='revisions'").fetchone() is None):
        cur.execute("CREATE TABLE revisions ( \
            rev_id INT PRIMARY KEY, \
            page_id INT, \
            user VARCHAR, \
            timestamp DATETIME, \
            content VARCHAR, \
            comment VARCHAR, \
            FOREIGN KEY (page_id) REFERENCES content_pages(page_id) \
        );")
     
    cur.execute("SELECT * FROM content_pages")

    for page in cur.fetchall():
        if (cur.execute("SELECT * FROM revisions WHERE page_id=?", (str(page[0]),)).fetchone() is None): #TODO this is hacky don't keep
            print("Extracting: %s" % page[0])
            get_revisions_for_page_id(page[0], cur)
        else:
            print("Already processed: %s" % page[0])
    
    con.commit()
    con.close()

# We format so that categories which are malformed are still tracked
def format_category_string(string):
    return string.strip().lower().replace("_", " ").replace("-", " ")

# Regex is truely disgusting...
CATEGORY_TAG = r"\[\[Category:([\w -]+)(?:|[^\]]+)?\]\]"
LMW_TEMPLATE_CATEGORY = r"{{LMW(?:\n*\|\w+=[^|]+)+\n*\|status=([\w -]+)(?:\n*\|\w+=[^|]+)*\n*}}"

LMW_TO_CATEGORY_TAG = {"found"           : "found media",
                       "lost"            : "completely lost media",
                       "partially lost"  : "partially lost media",
                       "partially found" : "partially found media"}

# Weird categories (special characters, empty categories etc.) are just ignored
def parse_categories(revision_id, cur):
    revision_content_tuple = cur.execute("SELECT content FROM revisions WHERE rev_id=?", (str(revision_id),)).fetchone()
    revision_content = (revision_content_tuple[0] if revision_content_tuple[0] else "")

    category_tags = re.findall(CATEGORY_TAG, revision_content)
    for category in category_tags:
        formated = format_category_string(category)
        cur.execute("INSERT OR IGNORE INTO categories (rev_id, category) values (?, ?)", (revision_id, formated))

    lmw_template_status = re.search(LMW_TEMPLATE_CATEGORY, revision_content)
    if lmw_template_status:
        formated = format_category_string(lmw_template_status.group(1))
        if formated in LMW_TO_CATEGORY_TAG.keys():
            formated = LMW_TO_CATEGORY_TAG[format_category_string(lmw_template_status.group(1))]
            cur.execute("INSERT OR IGNORE INTO categories (rev_id, category) values (?, ?)", (revision_id, formated))
    
def parse_categories_for_all_revisions():
    con = sqlite3.connect("lostmediawiki.db")
    cur = con.cursor()

    if (cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='categories'").fetchone() is None):
        cur.execute("CREATE TABLE categories ( \
            rev_id INT, \
            category VARCHAR, \
            PRIMARY KEY (rev_id, category), \
            FOREIGN KEY (rev_id) REFERENCES revisions(rev_id) \
        );")
    
    cur.execute("SELECT rev_id FROM revisions")

    for rev_id in cur.fetchall():
        if (cur.execute("SELECT * FROM categories WHERE rev_id=?", (str(rev_id[0]),)).fetchone() is None): #TODO this is hacky don't keep
            print("Parsing: %s" % rev_id[0])
            parse_categories(rev_id[0], cur)
        else:
            print("Already processed: %s" % rev_id[0])

    con.commit()
    con.close()

def create_table_if_empty(name, schema, cur):
    if (cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
                     .fetchone() is None):
        cur.execute("CREATE TABLE {} ({});".format(name, schema))

# Incorrect combinations:
# non-existance+completely lost
#SELECT t1.category as item1, t2.category as item2, COUNT(*) as cnt FROM categories t1, categories t2 on t1.rev_id = t2.rev_id WHERE t1.category < t2.category AND ("existence unconfirmed" IN (t1.category, t2.category) OR "non aexistence confirmed" in (t1.category, t2.category)) GROUP BY t1.category, t2.category ORDER BY COUNT(*);

# "non-existance confirmed" could mean partially or completely, going by the "3D Groove Games" article
def status_whitelist(cur):
    schema = "status VARCHAR PRIMARY KEY"
    create_table_if_empty("status_whitelist", schema, cur)

    STATUSES = ["completely lost media",
                "found media",
                "partially found media",
                "partially lost media"]

    for status in STATUSES:
        cur.execute("INSERT INTO status_whitelist (status) VALUES (?)", (status,))

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# Now the fun really starts
def status_changes():
    LATEST_REVISIONS_ON_DATE = "SELECT t1.* FROM losted_revisions t1, " \
                               "(SELECT page_id, MAX(timestamp) timestamp FROM revisions " \
                                                        "WHERE timestamp <= %s GROUP BY page_id) " \
                                "t2 ON t1.page_id = t2.page_id AND t1.timestamp = t2.timestamp"

    STATUS_COUNTS_ON_DATE =    "SELECT category, COUNT(*) FROM ({}) t1, categories t2 ON t1.rev_id = t2.rev_id " \
                               "WHERE category IN (SELECT * FROM status_whitelist) GROUP BY category" \
                                .format(LATEST_REVISIONS_ON_DATE)

    con = sqlite3.connect("lostmediawiki.db")
    cur = con.cursor()

    SCHEMA = "category  VARCHAR,  \
              count     INT,      \
              timestamp DATETIME, \
              PRIMARY KEY (category, count, timestamp)"
    create_table_if_empty("status_counts_overtime", SCHEMA, cur)

    earliest_date = datetime(day=10, month=6, year=2014)
    latest_date = datetime(day=4, month=7, year=2023)

    for timestamp in daterange(earliest_date, latest_date):
        timestamp = timestamp.strftime("%Y-%d-%m")
        query = STATUS_COUNTS_ON_DATE % ('"'+str(timestamp)+'"')
        status_counts = cur.execute(query).fetchall()
        for status_count in status_counts:
            cur.execute("INSERT INTO status_counts_overtime (category, count, timestamp) \
                         VALUES (?, ?, ?)", (status_count[0], status_count[1], timestamp))

    con.commit()
    con.close()

def status_counts_graph():
    con = sqlite3.connect("lostmediawiki.db")
    data = pd.read_sql("SELECT * FROM status_counts_overtime", con, parse_dates=("timestamp"))

    plot = (ggplot(data, aes(x="timestamp", y="count", color="category")) + geom_area()
                   + scale_x_datetime(date_breaks="1 years", date_labels="%Y"))

    plot.save("aaaaaa.png", dpi=400)

# Before May 2020, lost articles were identified by omission of the other status tags.
# To track these older revisions, we tag them in categories by extrapolating from
# when they were refactored in May 2020 to have explicit lost tags.

# TODO: a better way of doing this is to probably look for articles which are in categories (Lost TV, Lost WhATeVr)
# and also do not have status tags. If precision is an issue then we look at that.
def extrapolate_older_lost_articles():

    LOST_REFACTORS = "SELECT page_id FROM losted_revisions t1, categories t2 ON t1.rev_id = t2.rev_id WHERE category = 'completely lost media' AND timestamp >= '2019-01-17' AND timestamp <= '2020-05-16' GROUP BY page_id"

    con = sqlite3.connect("lostmediawiki.db")
    cur = con.cursor()
    lost_page_ids = cur.execute(LOST_REFACTORS).fetchall()

    for page_id in lost_page_ids:
        past_revisions = cur.execute("SELECT rev_id FROM losted_revisions WHERE page_id = ? AND timestamp <= '2020-05-16'",
                                     (page_id[0],)).fetchall()
        for revision in past_revisions:
            cur.execute("INSERT OR IGNORE INTO categories (rev_id, category) VALUES (?, ?)",
                         (revision[0], "completely lost media"))

    con.commit()
    con.close()

# Any pages with more than 50 revisions? 51st and onward revisions were missed out, you fuck!
if __name__ == "__main__":
    status_counts_graph()
