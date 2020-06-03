import os
import csv
import requests
import mysql.connector
from datetime import datetime

def fullIBGE(cod6: str):
    a = int(cod6[0])
    b = (int(cod6[1]) * 2) % 10 + (int(cod6[1]) * 2) // 10
    c = int(cod6[2])
    d = (int(cod6[3]) * 2) % 10 + (int(cod6[3]) * 2) // 10
    e = int(cod6[4])
    f = (int(cod6[5]) * 2) % 10 + (int(cod6[5]) * 2) // 10
    digit = (10 - (a + b + c + d + e + f) % 10) % 10
    return cod6 + str(digit)

# save today's date
today = datetime.today().strftime('%Y-%m-%d')

# set BD
mydb = mysql.connector.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    passwd=os.environ['DB_PASS'],
    database=os.environ['DB_NAME']
)
cursor = mydb.cursor()
select_sql = 'SELECT Outbreaks.idOutbreak as idOutbreak \
            FROM Outbreaks INNER JOIN Cities \
                ON Outbreaks.CityID = Cities.cityID \
            INNER JOIN Diseases \
                ON Outbreaks.DiseaseID = Diseases.idDisease \
            WHERE Diseases.idDisease = 5 and Outbreaks.Date = "'
update_sql = 'UPDATE Outbreaks \
            SET \
	            NumberOfCases = %s, \
	            FatalCases = %s, \
                Date = %s \
	        WHERE idOutbreak = %s;'
insert_sql = 'INSERT INTO Outbreaks (NumberOfCases, FatalCases, DiseaseID, Date, CityID) \
            VALUES (%s, %s, "5", %s, %s);'


# regiao;estado;municipio;coduf;codmun;codRegiaoSaude;nomeRegiaoSaude;data;semanaEpi;populacaoTCU2019;casosAcumulado;casosNovos;obitosAcumulado;obitosNovos;Recuperadosnovos;emAcompanhamentoNovos
csv_file = open('downloaded.csv', encoding="ISO-8859-1")
lines = csv_file.readlines()[1:-1]
reader = csv.reader(lines, delimiter=',')

for row in reader:
    if reader.line_num < 130000:
        continue
    elif reader.line_num > 129999:
        break

    print("Updating line number: " + str(reader.line_num + 1))
    ibge_cod = fullIBGE(row[4])
    date = row[7]
    total_cases = row[10]
    total_deaths = row[12]
    current_select_sql = select_sql + date + '" and Cities.cityID = ' + ibge_cod + ";"
    cursor.execute(current_select_sql)
    outbreak_id = cursor.fetchall()
        
    if len(outbreak_id) == 0:
        val = (total_cases, total_deaths, date, ibge_cod)
        cursor.execute(insert_sql, val)

    mydb.commit()
