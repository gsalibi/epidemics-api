import os
import csv
import xlrd
import requests
import mysql.connector
from datetime import datetime


# Save file from url
url = 'https://www.seade.gov.br/wp-content/uploads/2020/05/Dados-covid-19-municipios.csv'
req = requests.get(url)
url_content = req.content
csv_file = open('downloaded.csv', 'wb')
csv_file.write(url_content)
csv_file.close()

# save today date
today = datetime.today().strftime('%Y-%m-%d')

# set BD
mydb = mysql.connector.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    passwd=os.environ['DB_PASS'],
    database=os.environ['DB_NAME']
)
mycursor = mydb.cursor()
select_sql = 'SELECT Outbreaks.idOutbreak as idOutbreak \
            FROM Outbreaks INNER JOIN Cities \
                ON Outbreaks.CityID = Cities.cityID \
            INNER JOIN Diseases \
                ON Outbreaks.DiseaseID = Diseases.idDisease \
            WHERE Diseases.idDisease = 5 and Outbreaks.Date = ' + today + ' and Cities.cityID = '
update_sql = 'UPDATE Outbreaks \
            SET \
	            NumberOfCases = %s, \
	            FatalCases = %s, \
                Date = %s \
	        WHERE idOutbreak = %s;'
insert_sql = 'INSERT INTO Outbreaks (NumberOfCases, FatalCases, DiseaseID, Date, CityID) \
            VALUES (%s, %s, "5", %s, %s);'


# Cod_IBGE;Grande região;Município;Mun_Total de casos;Mun_Total de óbitos;;
csv_file = open('downloaded.csv', encoding="ISO-8859-1")
csv_file = csv_file.readlines()[1:-1]
csv_file = csv.reader(csv_file, delimiter=';')

for row in csv_file:
    print("Updating line number: " + str(csv_file.line_num))
    ibge_cod = row[0]
    total_cases = row[3]
    total_deaths = row[4]

    mycursor.execute(select_sql + str(ibge_cod) + ";")
    outbreak_id = mycursor.fetchall()

    if len(outbreak_id) > 0:
        val = (total_cases, total_deaths, today, outbreak_id[0][0])
        mycursor.execute(update_sql, val)
    else:
        val = (total_cases, total_deaths, today, ibge_cod)
        mycursor.execute(insert_sql, val)

    mydb.commit()
