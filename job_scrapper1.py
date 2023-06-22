import requests
from bs4 import BeautifulSoup
import mysql.connector

# job scrapper for karir.com
# filter 2 bulan setelah publikasi blm
job_type = ["programmer", "data", "network", "cyber security"]
all_job = {}

for type in job_type:

    job_list = []
    # initialization
    url = "https://karir.com/search?q="+type + \
        "&sort_order=urgent_job&job_function_ids=&industry_ids=&degree_ids=&major_ids=&location_ids=&location_id=&location=&salary_lower=0&salary_upper=100000000&page=1"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    # get the number of page
    pagination = soup.find(
        "div", class_="search-pagination sticky-search-stopper")
    pages = pagination.find_all('li')  # to get the maximum number of page
    max_page = pages[-3].find('a').text.strip()

    # scrapping first page
    cards = soup.find_all("li", class_="columns opportunity")
    for card in cards:
        card_data = {}
        card_data["title"] = card.find('h4').text.strip()
        card_data["date"] = card.find('time').get('datetime')
        card_data["location"] = card.find(
            'span', class_='tdd-location').text.strip()
        card_data["company"] = card.find(
            'div', class_='tdd-company-name').text.strip()
        card_data["link"] = "https://karir.com" + \
            card.find('a', class_="--blue").get('href')
        job_list.append(card_data)

    # scrapping the rest of page
    for page in range(2, int(max_page)+1):
        plus_type = type.replace(" ", "+")
        url = "https://karir.com/search?q="+plus_type + \
            "&sort_order=urgent_job&job_function_ids=&industry_ids=&degree_ids=&major_ids=&location_ids=&location_id=&location=&salary_lower=0&salary_upper=100000000&page=" + \
            str(page)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        cards = soup.find_all("li", class_="columns opportunity")
        # print(cards)
        for card in cards:
            card_data = {}
            card_data["title"] = card.find('h4').text.strip()
            card_data["date"] = card.find('time').get('datetime')
            card_data["location"] = card.find(
                'span', class_='tdd-location').text.strip()
            card_data["company"] = card.find(
                'div', class_='tdd-company-name').text.strip()
            card_data["link"] = "https://karir.com" + \
                card.find('a', class_="--blue").get('href')
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
mycursor.execute("truncate programmer;")  # buat clear tabel
mycursor.execute("truncate data;")  # buat clear tabel
mycursor.execute("truncate network;")  # buat clear tabel
mycursor.execute("truncate cybersecurity;")  # buat clear tabel
# buat bikin tabel
# mycursor.execute("CREATE TABLE programmer ( title VARCHAR(255) NOT NULL, date DATETIME NULL, location VARCHAR(255) NULL, company VARCHAR(255) NULL, source VARCHAR(255) NULL, link VARCHAR(255) NULL);")
for type in job_type:
    clean_type = type.replace(" ", "")  # remove white space
    print(clean_type)
    sql = "INSERT INTO " + clean_type + \
        " (title, date, location, company, source, link) VALUES (%s, %s, %s, %s, %s, %s)"
    val = []
    for job in all_job[type]:
        val.append((job["title"], job["date"], job["location"],
                   job["company"], "karir.com", job["link"]))

    mycursor.executemany(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "was inserted.")
