import requests
import csv
from bs4 import BeautifulSoup
import mysql.connector
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

job_type = ["programmer", "data", "network", "cyber-security"]
all_job = {}

for type in job_type:
    job_list = []

    # get max page
    url = "https://www.kalibrr.com/job-board/te/" + type + "/1"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    searchPages = soup.find_all("li", class_="k-mx-2")
    page = []
    for j in searchPages:
        page.append(j.find("a").text.strip())

    max_page = page[-1]

    for page in range(1, int(max_page)+1):
        url = "https://www.kalibrr.com/job-board/te/" + type + "/" + str(page)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        search = soup.find_all(
            "div", class_="k-grid k-border-tertiary-ghost-color k-text-sm k-p-4 md:k-p-6 css-1b4vug6")
        for job in search:
            data = {}
            data['title'] = job.find(
                "a", class_="k-text-primary-color").text.strip()

            date = job.find(
                "span", class_="k-block k-mb-1").text.strip().split(" ")
            if (date[2] == 'days'):
                data['date'] = datetime.datetime.now() - \
                    timedelta(days=int(date[1]))
            elif (date[2] == 'day'):
                data['date'] = datetime.datetime.now() - \
                    timedelta(days=1)
            elif (date[2] == 'hours'):
                data['date'] = datetime.datetime.now(
                ) - timedelta(hours=int(date[1]))
            elif (date[2] == 'minutes'):
                data['date'] = datetime.datetime.now(
                ) - timedelta(minutes=int(date[1]))
            elif (date[2] == 'month'):
                data['date'] = datetime.datetime.now(
                ) - relativedelta(months=+1)
            elif (date[2] == 'months'):
                data['date'] = datetime.datetime.now(
                ) - relativedelta(months=+int(date[1]))
            elif (date[2] == 'year'):
                data['date'] = datetime.datetime.now(
                ) - relativedelta(years=+1)
            elif (date[2] == 'years'):
                data['date'] = datetime.datetime.now(
                ) - relativedelta(years=+int(date[1]))

            data['company'] = job.find(
                "span", class_="k-inline-flex k-items-center k-mb-1").text.strip()
            data['location'] = job.find(
                class_="k-text-subdued k-block").text.strip()
            data['link'] = "https://www.kalibrr.com" + \
                job.find("a", class_="k-text-primary-color").get('href')
            job_list.append(data)

    all_job[type] = job_list
    print(len(job_list))

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="job_EAI"
)

mycursor = mydb.cursor()
# mycursor.execute("truncate programmer;") #buat clear tabel
# buat bikin tabel
# mycursor.execute("CREATE TABLE data ( title VARCHAR(255) NOT NULL, date DATETIME NULL, location VARCHAR(255) NULL, company VARCHAR(255) NULL, source VARCHAR(255) NULL, link VARCHAR(255) NULL);")

for type in job_type:
    clean_type = type.replace("-", "")  # remove white space
    print(clean_type)
    sql = "INSERT INTO " + clean_type + \
        " (title, date, location, company, source, link) VALUES (%s, %s, %s, %s, %s, %s)"
    val = []
    for job in all_job[type]:
        val.append((job["title"], job["date"], job["location"],
                   job["company"], "kalibrr.com", job["link"]))

    mycursor.executemany(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "was inserted.")
