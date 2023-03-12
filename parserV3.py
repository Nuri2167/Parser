import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import dateparser
import sqlite3

# create connection and cursor to SQLite database
conn = sqlite3.connect('news.db')
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Resources")
cursor.execute("DROP TABLE IF EXISTS Items")
# create Resources table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Resources (
        RESOURCE_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        RESOURCE_NAME VARCHAR(255) NULL,
        RESOURCE_URL VARCHAR(255) NULL,
        top_tag VARCHAR(255) NOT NULL,
        bottom_tag VARCHAR(255) NOT NULL,
        title_cut VARCHAR(255) NOT NULL,
        date_cut VARCHAR(255) NOT NULL
    )
''')

# create Items table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        res_id INTEGER(11) REFERENCES Resources(RESOURCE_ID),
        link VARCHAR(255) NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        nd_date INTEGER(11) NOT NULL,
        s_date INTEGER(11) NOT NULL,
        not_date DATE NOT NULL
    )
''')

# insert data into Resources table
cursor.execute('''
    INSERT INTO Resources (RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut)
    VALUES ("Scientific Russia", "https://scientificrussia.ru/news", "list-item", "lead", "title", "time")
''')

# cursor.execute('''
#     INSERT INTO Resources (RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut)
#     VALUES ("Nur.kz", "https://www.nur.kz/society/", "li", "div", "h1", "time")
# ''')

conn.commit()

# function to extract news from a given resource
def parse_news(res_id, resource_url, top_tag, bottom_tag, title_cut, date_cut):
    # request the website
    res = requests.get(resource_url)
    soup = BeautifulSoup(res.content, 'html.parser')

    # extract all news links from the website
    news_links = soup.find_all(class_=top_tag)

    # iterate over each news link and extract the required information
    for link in news_links:
        try:
            # get link, title and content of the news
            news_link = link.find(class_=title_cut).find("a").get("href")
            news_title = link.find(class_=title_cut).find("a").text.strip()
            news_content = link.find(class_=bottom_tag).text.strip()

            # get date of the news and convert it to Unix time
            news_date_str = link.find(class_=date_cut).text.strip()
            news_date = dateparser.parse(news_date_str)
            news_date_unix = int(time.mktime(news_date.timetuple()))

            # insert the extracted information into Items table
            current_time_unix = int(time.time())
            current_date_str = news_date.strftime('%d/%m/%Y')

            connect = sqlite3.connect('news.db')
            c = connect.cursor()

            sql = "INSERT INTO Items (res_id, link, title, content, nd_date, s_date, not_date) VALUES (?, ?, ?, ?, ?, ?, ?)"
            val = (res_id, news_link, news_title, news_content, news_date_unix, current_time_unix, current_date_str)
            try:
                c.execute(sql, val)
                connect.commit()
            except sqlite3.Error as error:
                print("Error occurred while inserting data: ", error)

        except:
            continue

# iterate over each resource and extract news from it
cursor.execute('SELECT RESOURCE_ID, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut FROM Resources')
resources = cursor.fetchall()

for resource in resources:
    res_id, resource_url, top_tag, bottom_tag, title_cut, date_cut = resource
    parse_news(res_id, resource_url, top_tag, bottom_tag, title_cut, date_cut)

conn.close()

