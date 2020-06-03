from bs4 import BeautifulSoup
import requests
import csv
import datetime
import mysql.connector
import os


def fullIBGE(cod6: str):
    a = int(cod6[0])
    b = (int(cod6[1]) * 2) % 10 + (int(cod6[1]) * 2) // 10
    c = int(cod6[2])
    d = (int(cod6[3]) * 2) % 10 + (int(cod6[3]) * 2) // 10
    e = int(cod6[4])
    f = (int(cod6[5]) * 2) % 10 + (int(cod6[5]) * 2) // 10
    digit = (10 - (a + b + c + d + e + f) % 10) % 10
    return cod6 + str(digit)


def convert_to_css(data_path: str):
    with open(data_path, encoding='cp1252') as html_file:
        soup = BeautifulSoup(html_file, 'lxml', from_encoding="cp1252")

    match = soup.find_all(name='td')

    text = "DRS,GVE,Região de Saúde, município,SE01,SE02,SE03,SE04,SE05,SE06,SE07,SE08,SE09,SE10,SE11,SE12,SE13,SE14,SE15,SE16,SE17,SE18,TOTAL\n"
    for i in range(66, len(match) - 28, 26):
        try:
            int(str(match[i + 6].text).split(' ')[0])
        except ValueError:
            continue
        text += match[i + 0].text
        # ignore 1
        text += ","
        text += match[i + 2].text
        # ignore 3
        text += ","
        text += match[i + 4].text
        # ignore 5
        text += ","
        text += fullIBGE(str(match[i + 6].text).split(' ')[0])
        text += ","

        # add week values and sum total
        total_sum = 0
        for j in range(7, 25):
            total_sum += int(match[i + j].text.replace('.', ''))
            text += str(total_sum) + ','

        text += str(match[i + 25].text).replace('.', '')
        text += "\n"
    csv_file = open('dengue_sp.csv', 'wb')
    csv_file.write(text.encode())
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
    insert_sql = 'INSERT INTO Outbreaks (NumberOfCases, FatalCases, DiseaseID, Date, CityID) \
                VALUES (%s, %s, "1", %s, %s);'

    # get last update dates
    cursor.execute(select_sql)
    lastUpdate = cursor.fetchall()
    mydb.commit()

    # DRS,GVE,Região de Saúde, município,SE01,SE02,SE03,SE04,SE05,SE06,SE07,SE08,SE09,SE10,SE11,SE12,SE13,SE14,SE15,SE16,SE17,SE18,TOTAL
    csv_file = open(csv_path)
    lines = csv_file.readlines()[1:]
    reader = csv.reader(lines, delimiter=',')

    # set dates by weeks
    date = []
    for i in range(18):
        week = "2020-W" + str(i)
        date.append(datetime.datetime.strptime(week + '-6', "%Y-W%W-%w").strftime('%Y-%m-%d'))

    for row in reader:
        ibge_cod = row[3]
        for i in range(4, 22):
            total_cases = row[i]
            
            if date[i - 4] > lastUpdate[0][0].strftime('%Y-%m-%d'):
                print("Updating line number: " + str(reader.line_num + 1))
                val = (total_cases, None, date[i - 4], ibge_cod)
                cursor.execute(insert_sql, val)

        mydb.commit()

#convert_to_css("data.htm")

insert_csv_into_DB('dengue_sp.csv')

