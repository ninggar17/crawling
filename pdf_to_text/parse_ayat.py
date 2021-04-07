import json
import os
import re

import pandas

# file_list = open('C:/Users/Bukalapak/PycharmProjects/crawling/crawl/pdf_list', 'r+')
file_num = 0
state = True
res = None
line = 0
failed = []
while state:
    text = 'http://peraturan.go.id/common/dokumen/ln/1963/pp0271963.pdf'
    # text = file_list.readline()
    print(line, text)
    line += 1
    if text == '':
        state = False
    text = text.strip()
    filesource = 'E:/Ninggar/Mgstr/Semester 2/Data/files/parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '.csv', re.sub('(%20)|([\\._]+)', '_',
                                                              re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                     '', text)))).group(1).lower()) + '.csv'
    filetarget = 'E:/Ninggar/Mgstr/Semester 2/Data/files/ayat_parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '.csv', re.sub('(%20)|([\\._]+)', '_',
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
        peraturan = pandas.read_csv(filesource)
        # peraturan = pandas.read_csv('D:/Ninggar/Mgstr/Semester 2/Data/files/parsed_files/bn/2019/bn_498-2019.csv.csv')
    except FileNotFoundError:
        continue
    try:
        ayat = []
        num_ayat = []
        # for partition in peraturan.iterrows():
        #     print(partition)
        for partition in peraturan.iterrows():
            value = str(partition[1]['value'])
            if partition[1]['part_1'] in ['tentang', 'menimbang', 'mengingat', 'memutuskan']:
                ayat.append([value])
                num_ayat.append([])
                continue
            split = re.split(r'aya[tl]\s+\(\s*\d+\s*\)|\(\s*\d+\s*\)', value)
            # print('len_split',len(split))
            # del_split = []
            # print(split)
            ayat.append([x.strip() for x in split])
            num = re.findall(r'aya[tl]\s+\(\s*\d+\s*\)|\(\s*\d+\s*\)', value)
            # print(len(num))
            ayat_num = []
            # print(num)
            # print(json.dumps(ayat,indent=4))
            # print(partition[1]['partition'])
            # print(value)
            # print(num)
            for x in num:
                # print(x)
                if not re.search(r'(aya[tl]\s+\()\s*\d+\s*\)', x):
                    ayat_num.append(re.search(r'\d+', x).group(0))
                elif not re.search(r'aya[lt]\s+\($', x):
                    ayat_num.append(x)
            # print(ayat_num)
            num_ayat.append(ayat_num)
            # print(num_ayat)
        peraturan['ayat'] = ayat
        peraturan['num'] = num_ayat
        ayat = []
        num_ayat = []
        for row in peraturan.iterrows():
            print(row)
        for i in range(len(peraturan)):
            # print(peraturan.iloc[i]['part_2'])
            value = peraturan.iloc[i]['value']
            partition = peraturan.iloc[i]['part_1']
            row_ayat = peraturan.iloc[i]['ayat']
            row_ayat_num = peraturan.iloc[i]['num']
            len_ayat = len(row_ayat)
            len_ayat_num = len(row_ayat_num)
            # print(value)
            # print(value)
            # print('<<', row_ayat_num)
            # print('>>',json.dumps(row_ayat, indent=4))
            if len_ayat > 0 and len_ayat_num > 0 and re.search('aya[lt]', row_ayat_num[len_ayat_num - 1]) and re.search(
                    r'aya[lt]\s*\(\s*\d+\s*\)\W*$', value):
                row_ayat[len_ayat - 1] += ' ' + row_ayat_num[len_ayat_num - 1]
                row_ayat_num = row_ayat_num[:len_ayat_num - 1]
                len_ayat_num -= 1

            fake_ayat = 0
            for num in row_ayat_num:
                if re.search('aya[tl]', num):
                    fake_ayat += 1
            if fake_ayat != len_ayat_num and not re.search('aya[lt]', row_ayat_num[0]) and len_ayat == len_ayat_num + 1:
                # print(len_ayat, len_ayat_num)
                # print(len(ayat))
                # print(ayat[len(ayat) - 1][len(ayat[len(ayat) - 1])], re.search(r'.+\.$', ayat[len(ayat) - 1][len(ayat[len(ayat) - 1]) - 1]))
                if len(ayat) > 0\
                        and len(ayat[len(ayat) - 1]) != 0:
                    ayat[len(ayat) - 1][len(ayat[len(ayat) - 1]) - 1] += ' ' + row_ayat[0]
                    row_ayat = row_ayat[1:]
                else:
                    ayat[len(ayat) - 1].append(row_ayat[0])
                    row_ayat = row_ayat[1:]
                # print(row_ayat)
                len_ayat -= 1
            ayat.append(row_ayat)
            num_ayat.append(row_ayat_num)
            print(json.dumps(row_ayat, indent=4))
            print(json.dumps(row_ayat_num, indent=4))
        peraturan['ayat'] = ayat
        peraturan['num'] = num_ayat
        print('>>', json.dumps(ayat, indent=4))
        ayat = []
        num_ayat = []
        # for row in peraturan.iterrows():
        #     print(row)
        for i in range(len(peraturan)):
            # print(peraturan.iloc[i]['partition'])
            value = peraturan.iloc[i]['value']
            row_ayat = peraturan.iloc[i]['ayat']
            row_ayat_num = peraturan.iloc[i]['num']
            len_ayat = len(row_ayat)
            len_ayat_num = len(row_ayat_num)
            del_part = []
            # print(value)
            # print(peraturan.iloc[i]['part_2'])
            # print(json.dumps(row_ayat, indent=4))
            # print(json.dumps(row_ayat_num, indent=4))
            # print(len_ayat, len_ayat_num)
            # print('==', row_ayat)
            # print('==', row_ayat_num)
            temp_idx = None
            if not (len_ayat == 0 and len_ayat_num == 1):
                # print('aaa')
                for part in range(len_ayat_num):
                    if re.search('aya[tl]', row_ayat_num[part]):
                        # print('xxx')
                        # print(i, part)
                        # print(len(row_ayat))
                        # print(len(row_ayat_num))
                        # print(len(row_ayat))
                        # print('<-', row_ayat[part - 1], temp_idx)
                        # print('xx',value)
                        # print('<--', row_ayat_num)
                        # print('<---', row_ayat)
                        if len_ayat_num >= len_ayat:
                            if temp_idx is None:
                                temp_idx = part - 1
                            row_ayat[temp_idx] += ' ' + row_ayat_num[part] + ' ' + row_ayat[part]
                            del_part.append(part)
                        else:
                            if temp_idx is None:
                                temp_idx = part
                            row_ayat[temp_idx] += ' ' + row_ayat_num[part] + ' ' + row_ayat[part + 1]
                            del_part.append(part + 1)
                        # print('-->',temp_idx, row_ayat[temp_idx])
                        # print('-->', row_ayat[part - 1])
                    else:
                        temp_idx = None
                for part in del_part[::-1]:
                    row_ayat.pop(part)
                    if len_ayat_num >= len_ayat:
                        row_ayat_num.pop(part)
                    else:
                        row_ayat_num.pop(part - 1)
                # print(json.dumps(row_ayat, indent=4))
                # print(json.dumps(row_ayat_num, indent=4))
        array_res = []
        for i in range(len(peraturan)):
            row_ayat = peraturan.iloc[i]['ayat']
            row_ayat_num = peraturan.iloc[i]['num']
            if len(row_ayat) > len(row_ayat_num) != 0:
                row_ayat_num = [None] + row_ayat_num
            for part in range(len(row_ayat)):
                try:
                    array_res.append({
                        'partition': peraturan.iloc[i]['partition'],
                        'filename': peraturan.iloc[i]['filename'],
                        'start_pos': peraturan.iloc[i]['start_pos'],
                        'part_1': peraturan.iloc[i]['part_1'],
                        'part_2': peraturan.iloc[i]['part_2'],
                        'ayat': row_ayat_num[part],
                        'value': row_ayat[part],
                    })
                except Exception:
                    array_res.append({
                        'partition': peraturan.iloc[i]['partition'],
                        'filename': peraturan.iloc[i]['filename'],
                        'start_pos': peraturan.iloc[i]['start_pos'],
                        'part_1': peraturan.iloc[i]['part_1'],
                        'part_2': peraturan.iloc[i]['part_2'],
                        'ayat': None,
                        'value': row_ayat[part],
                    })
        # print(array_res)
        res_df = pandas.DataFrame(array_res)
        # print(res_df)
        # print(filetarget)
        # res_df.to_csv(filetarget)
        # print(res_df[['part_2','ayat']])
        # print(filetarget)
    except Exception:
        failed.append(filesource)
        # state = False
        # print(Exception.with_traceback())
    break
print(json.dumps(failed, indent=4))
