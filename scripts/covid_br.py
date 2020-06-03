import os
import csv
import gzip
import requests
import mysql.connector
from datetime import datetime

def get_data():
    # Save file from url
    url = 'https://data.brasil.io/dataset/covid19/caso.csv.gz'
    req = requests.get(url)
    url_content =  gzip.decompress(req.content)

    csv_file = open('covid_br.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()

def insert_csv_into_DB(csv_path: str):
    # set BD
    mydb = mysql.connector.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        passwd=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']
    )
    cursor = mydb.cursor()
    select_sql = 'SELECT LastUpdate FROM Diseases ORDER BY idDisease;'
    update_sql = 'UPDATE Diseases SET LastUpdate = DATE("%s") WHERE idDisease = 5;'
    insert_sql = 'INSERT INTO Outbreaks (NumberOfCases, FatalCases, DiseaseID, Date, CityID) \
                VALUES (%s, %s, "5", %s, %s);'

    # get last update dates
    cursor.execute(select_sql)
    lastUpdate = cursor.fetchall()
    mydb.commit()

    # date,state,city,place_type,confirmed,deaths,order_for_place,is_last,estimated_population_2019,city_ibge_code,confirmed_per_100k_inhabitants,death_rate

    csv_file = open(csv_path)
    lines = csv_file.readlines()[1:]
    total_lines = len(lines) + 2 # index 0 + header line
    reader = csv.reader(reversed(lines), delimiter=',')
    new_update = lastUpdate[4][0].strftime('%Y-%m-%d')

    for row in reader:
        print("Current: " + str(total_lines - reader.line_num))
        date = row[0]
        total_cases = row[4]
        total_deaths = row[5]
        ibge_cod = row[9]
        
        if len(ibge_cod) > 2 and date > lastUpdate[4][0].strftime('%Y-%m-%d'):
            print("Updating line number: " + str(total_lines - reader.line_num))
            val = (total_cases, total_deaths, date, ibge_cod)
            cursor.execute(insert_sql, val)
            if date > new_update:
                new_update = date

    val = (new_update)
    cursor.execute(update_sql, val)
    mydb.commit()

get_data()
insert_csv_into_DB('covid_br.csv')
