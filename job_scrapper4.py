from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import requests
from bs4 import BeautifulSoup
import mysql.connector

chrome_options = Options()
chrome_options.headless = False
driver = webdriver.Chrome(
    options=chrome_options, executable_path='C:/Users/Salvian/Downloads/chromedriver_win32/chromedriver.exe')

job_type = ['programmer', 'data', 'network', 'cyber security']
all_job = {}

for type in job_type:
    uniq_type = type.replace(" ", "%20")
    job_url = "https://www.linkedin.com/jobs/search?keywords=" + uniq_type + \
        "&location=Indonesia&geoId=&trk=public_jobs_jobs-search-bar_search-submit"
    driver.get(job_url)
    time.sleep(2)

    # You can set your own pause time. My laptop is a bit slow so I use 1 sec
    scroll_pause_time = 1
    screen_height = driver.execute_script(
        "return window.screen.height;")   # get the screen height of the web
    i = 1

    while True:
        # scroll one screen height each time
        driver.execute_script(
            "window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))
        i += 1
        job_src = driver.page_source
        soup = BeautifulSoup(job_src, "html.parser")

        jobs = []
        search = soup.find("ul", class_="jobs-search__results-list")

        for li in search.find_all('li'):
            data = {}
            data['title'] = li.find(
                'h3', class_="base-search-card__title").text.strip()
            data['date'] = li.find('time').get(
                'datetime').replace("T", " ").replace(".000Z", "")
            data['location'] = li.find(
                'span', class_="job-search-card__location").text.strip()
            data['company'] = li.find(
                'h4', class_="base-search-card__subtitle").text.strip()
            link = li.find(
                "a", {"class": "base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]"})
            if link is None:
                data['link'] = "https://www.linkedin.com/"
            else:
                data['link'] = link.get("href")
            jobs.append(data)

        time.sleep(scroll_pause_time)

        # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
        scroll_height = driver.execute_script(
            "return document.body.scrollHeight;")
        # Break the loop when the height we need to scroll to is larger than the total scroll height
        if (screen_height) * i > scroll_height:
            all_job[type] = jobs
            print(len(jobs))
            break

    all_job[type] = jobs


print(len(all_job))

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
    clean_type = type.replace(" ", "")  # remove white space
    print(clean_type)
    sql = "INSERT INTO " + clean_type + \
        " (title, date, location, company, source, link) VALUES (%s, %s, %s, %s, %s, %s)"
    val = []
    for job in all_job[type]:
        val.append((job["title"], job["date"], job["location"],
                   job["company"], "LinkedIn", job["link"]))

    mycursor.executemany(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "was inserted.")
