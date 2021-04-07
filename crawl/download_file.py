import os
import re
import urllib.request

# file = open('failed_filename', 'r+')
# state = True
# failed = []
# file_num = 0
# while state:
#     text = file.readline()
#     if text == '':
#         state = False
#     text = text.strip()
#     filename = 'files/' + (
#         re.sub('_pdf', '.pdf', re.sub('(%20)|([\\._]+)', '_',
#                                       re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
#     dirName = re.sub('/[^/]+$', '', filename)
#     try:
#         # Create target Directory
#         os.makedirs(dirName)
#         print("Directory ", dirName, " Created ")
#     except FileExistsError:
#         None
#     try:
#         urllib.request.urlretrieve(text, filename)
#     except Exception:
#         failed.append(text)
#     print(file_num)
#     file_num += 1
# print(failed)
# file.close()
# urllib.request.urlretrieve('http://www.setpp.kemenkeu.go.id/risalah/ambilFileDariDisk/37507', "filename.pdf")
import requests

# r = requests.get('http://www.setpp.kemenkeu.go.id/risalah/ambilFileDariDisk/20000', allow_redirects=True)
r = requests.get('https://peraturan.go.id/common/dokumen/ln/2020/ps2-2020.pdf')
open('file-risalah-2.txt', 'wb').write(r.content)
