import json
import re

import pandas
import requests

try:
    old_data_df = pandas.read_csv('data_peraturan.csv')
except Exception:
    old_data_df = None
start_url = "http://peraturan.go.id//peraturan/index.html?page=2525"
url = start_url
# files = []
# file = open('pdf_list', 'a+')
data = []
while url is not None:
    print(url)
    r = requests.get(url=url)
    content = r.content.__str__()
    if r.status_code == 200:
        table = re.split('</tr>', re.sub('((\\\\[rnt])+)|(<tr data-key="[^"]+">)', '',
                                         re.search('<tbody>((\\\\[rnt]|\\s)*)(.+)((\\\\[rnt]|\\s)*)</tbody>',
                                                   content).group(3)))
        table.remove('')
        page_data = []
        for row in table:
            temp = re.split('</td>', re.sub('<td[^>]+>', '', row))
            temp[4] = re.findall('href="([^"]+)"', temp[3])
            files = []
            for file in temp[4]:
                files.append(re.sub('\\s', '%20', file))
                temp[4] = files
            temp[3] = temp[2]
            temp[2] = "http://peraturan.go.id" + re.search('href="([^"]+)"', temp[1]).group(1)
            temp[1] = re.search('>([^<]+)</a>', temp[1]).group(1)
            page_data.append(temp)
        data += page_data
    try:
        next_page = re.findall('<li class="next"><a href="(.+)"\\s*data-page="\\d+">&raquo', content)[0]
        url = "http://peraturan.go.id" + re.sub(';', '%3B', next_page)
    except Exception:
        last_url = url
        url = None

data_df = pandas.DataFrame(data, columns=['nomor', 'peraturan', 'url_peraturan', 'tentang', 'files'])
if old_data_df is not None:
    data_df = pandas.concat([old_data_df, data_df])
data_df.to_csv('data_peraturan.csv')
# file.close()
# print(files)
# raw_files = re.findall('(http(s?):[^"]+\\.pdf)', content)
# for tuple_file in raw_files:
#     print(tuple_file[0])

# import urllib.request
# urllib.request.urlretrieve('http://peraturan.go.id/common/dokumen/bn/2013/bn671-2013lamp.pdf', "filename.pdf")
