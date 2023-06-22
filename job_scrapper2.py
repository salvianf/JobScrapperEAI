import requests
from bs4 import BeautifulSoup
import mysql.connector

# job scrapper for jobstreet.co.id
# filter 2 bulan setelah publikasi blm
job_type = ["programmer", "data", "network", "cyber security"]
all_job = {}

for type in job_type:

    job_list = []
    striped_type = type.replace(" ", "-")
    # initialization
    url = "https://www.jobstreet.co.id/id/"+type + \
        "-jobs?createdAt=30d"  # filter 30 hari
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    # get the number of page
    pagination = soup.find(
        "select", id="pagination")
    pages = pagination.find_all('option')  # to get the maximum number of page
    max_page = pages[-1].get('value')

    # scrapping first page
    cards = soup.find_all(
        "div", class_="z1s6m00 _1hbhsw67i _1hbhsw66e _1hbhsw69q _1hbhsw68m _1hbhsw6n _1hbhsw65a _1hbhsw6ga _1hbhsw6fy")

    for card in cards:
        card_data = {}
        card_data["title"] = card.find('span', class_="z1s6m00").text.strip()
        card_data["date"] = card.find('time').get(
            'datetime').replace("T", " ").replace(".000Z", "")
        card_data["location"] = card.find(
            'span', class_='z1s6m00 _1hbhsw64y y44q7i0 y44q7i3 y44q7i21 y44q7ih').text.strip()
        card_data["company"] = card.find(
            'span', class_='z1s6m00 bev08l1 _1hbhsw64y _1hbhsw60 _1hbhsw6r').text.strip()
        card_data["link"] = "https://www.jobstreet.co.id" + card.find(
            'a', class_="jdlu994 jdlu996 jdlu999 y44q7i2 z1s6m00 z1s6m0f _1hbhsw6h").get('href')
        job_list.append(card_data)

    # scrapping the rest of page
    for page in range(2, int(max_page)+1):
        url = "https://www.jobstreet.co.id/id/" + \
            type + "-jobs?createdAt=30d&pg=" + str(page)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        cards = soup.find_all(
            "div", class_="z1s6m00 _1hbhsw67i _1hbhsw66e _1hbhsw69q _1hbhsw68m _1hbhsw6n _1hbhsw65a _1hbhsw6ga _1hbhsw6fy")

        for card in cards:
            card_data = {}
            card_data["title"] = card.find(
                'span', class_="z1s6m00").text.strip()
            card_data["date"] = card.find('time').get(
                'datetime').replace("T", " ").replace(".000Z", "")
            card_data["location"] = card.find(
                'span', class_='z1s6m00 _1hbhsw64y y44q7i0 y44q7i3 y44q7i21 y44q7ih').text.strip()
            card_data["company"] = card.find(
                'span', class_='z1s6m00 bev08l1 _1hbhsw64y _1hbhsw60 _1hbhsw6r').text.strip()
            card_data["link"] = "https://www.jobstreet.co.id" + card.find(
                'a', class_="jdlu994 jdlu996 jdlu999 y44q7i2 z1s6m00 z1s6m0f _1hbhsw6h").get('href')
            job_list.append(card_data)

    all_job[type] = job_list

# initialize connection to database
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
                   job["company"], "jobstreet.co.id", job["link"]))

    mycursor.executemany(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "was inserted.")
