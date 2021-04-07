import json
import os
import re

import pandas

file_list = open('C:/Users/Bukalapak/PycharmProjects/crawling/crawl/pdf_list', 'r+')
file_num = 0
state = True
res = None
line = 0
failed = []


def roman_to_int(s):
    """

    :param s:string romawi
    :return: integer
    """
    rom_val = {'i': 1, 'v': 5, 'x': 10, 'l': 50, 'c': 100, 'd': 500, 'm': 1000}
    int_val = 0
    for i in range(len(s)):
        if i > 0 and rom_val[s[i]] > rom_val[s[i - 1]]:
            int_val += rom_val[s[i]] - 2 * rom_val[s[i - 1]]
        else:
            int_val += rom_val[s[i]]
    return int_val


def latin_to_int(s):
    """

    :param s:string romawi
    :return: integer
    """
    words = re.split(r'\s+', s)
    words = [x.strip() for x in words if x.strip() != '']
    latin_val = {
        'satu': 1,
        'dua': 2,
        'tiga': 3,
        'empat': 4,
        'lima': 5,
        'enam': 6,
        'tujuh': 7,
        'delapan': 8,
        'sembilan': 9,
        'sepuluh': 10,
        'sebelas': 11,
        'seratus': 100,
        'seribu': 1000
    }
    int_val = 0
    vals = []
    for i in range(len(words)):
        if words[i] == 'belas':
            vals[len(vals) - 1] += 10
        elif words[i] == 'puluh':
            vals[len(vals) - 1] *= 10
        elif words[i] == 'ratus':
            vals[len(vals) - 1] *= 100
        elif words[i] == 'ribu':
            vals[len(vals) - 1] *= 1000
        elif words[i] == 'juta':
            vals[len(vals) - 1] *= 1000000
        else:
            vals.append(latin_val[words[i]])
    for val in vals:
        int_val += val
    return int_val


while state:
    text = file_list.readline()
    # text='hhttp://peraturan.go.id/common/dokumen/bn/2011/bn173-2011.pdf'
    print(line, text)
    line += 1
    if text == '':
        state = False
    text = text.strip()
    filesource = 'D:/Ninggar/Mgstr/Semester 2/Data/files/fix_position_v1/' + (
        re.search(r'(.{,100})', re.sub('(%20)|([\\._]+)', '_', re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                      '', text)))).group(1).lower() + '.csv'
    filetarget = 'D:/Ninggar/Mgstr/Semester 2/Data/files/fix_position_v2/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '', re.sub('(%20)|([\\._]+)', '_',
                                                              re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                     '', text)))).group(1).lower()) + '.csv'
    dirName = re.sub('/[^/]+$', '', filetarget)
    try:
        # Create target Directory
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except Exception:
        pass
    try:
        data = pandas.read_csv(filesource)
        # data = pandas.read_csv('D:/Ninggar/Mgstr/Semester 2/Data/files/ayat_parsed_files/perda/2018/2018-33.csv')
    except FileNotFoundError:
        print('not found')
        continue

    print(filesource)
    print(filetarget)
    break
