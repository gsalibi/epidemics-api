from bs4 import BeautifulSoup
import requests
import csv
import datetime


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
        text += " "
        text += match[i + 1].text
        text += ","
        text += match[i + 2].text
        text += " "
        text += match[i + 3].text
        text += ","
        text += match[i + 4].text
        text += " "
        text += match[i + 5].text
        text += ","
        text += fullIBGE(str(match[i + 6].text).split(' ')[0])
        text += ","
        text += match[i + 7].text
        text += ","
        text += match[i + 8].text
        text += ","
        text += match[i + 9].text
        text += ","
        text += match[i + 10].text
        text += ","
        text += match[i + 11].text
        text += ","
        text += match[i + 12].text
        text += ","
        text += match[i + 13].text
        text += ","
        text += match[i + 14].text
        text += ","
        text += match[i + 15].text
        text += ","
        text += match[i + 16].text
        text += ","
        text += match[i + 17].text
        text += ","
        text += match[i + 18].text
        text += ","
        text += match[i + 19].text
        text += ","
        text += match[i + 20].text
        text += ","
        text += match[i + 21].text
        text += ","
        text += match[i + 22].text
        text += ","
        text += match[i + 23].text
        text += ","
        text += match[i + 24].text
        text += ","
        text += str( match[i + 25].text).replace('.', '')
        text += "\n"
    csv_file = open('dengue_sp.csv', 'wb')
    csv_file.write(text.encode())
    csv_file.close()


# convert_to_css("data.htm")

d = "2020-W0" # week -1
r = datetime.datetime.strptime(d + '-6', "%Y-W%W-%w").strftime('%Y-%m-%d')

print(r)