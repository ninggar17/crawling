import os
import re

import pandas

file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
file_num = 0
state = True
res = None
failed = []
parsing = pandas.read_csv('parsed_result_partition.csv')[
    {'partition', 'start_pos', 'filename', 'part_1', 'part_2', 'state'}]
while state:
    text = file_list.readline()
    if text == '':
        state = False
    print(text)
    text = text.strip()
    filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_text_files/' + (
        re.sub('_pdf', '.txt', re.sub('(%20)|([\\._]+)', '_',
                                      re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
    filetarget = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '.csv', re.sub('(%20)|([\\._]+)', '_',
                                                              re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                     '', text)))).group(1).lower()) + '.csv'
    dirName = re.sub('/[^/]+$', '', filetarget)
    try:
        # Create target Directory
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except Exception:
        None
    try:
        # filesource = 'D:/Ninggar/Mgstr/Semester 2/Data/files/text_files/bn/2019/bn_1236-2019.txt'
        file = open(filesource)
    except Exception:
        continue
    if re.search('putusan', filesource):
        continue
    text = file.read()
    text = (re.sub('\\nwww.djpp.depkumham.go.id\\n', '\n',
                   re.sub('[^a-zA-Z\\d\\n./:(&-,);]', ' ',
                          re.sub('\\n+', '\n', re.sub('\\n\\s+\\n', '\\n\\n', text))))).lower().strip()

    try:
        partition = parsing[parsing['filename'] == re.sub('^E','D', filesource)]
        # partition. = [0, 'header', 0, partition.iloc[0]['filename'], 'header', None, 'clear']
        header = [{'partition': 'header', 'start_pos': 0, 'filename': partition.iloc[0]['filename'], 'part_1': 'header',
                   'part_2': None, 'state': 'clear'}]
        partition = pandas.concat([pandas.DataFrame(header), partition], ignore_index=True, sort=True)
        # print(partition.iloc[0])
        parts = []

        for index in range(len(partition)):
            # print(partition.iloc[index]['partition'])

            # print(partition.iloc[index]['part_2'])
            try:
                start = partition.iloc[index]['start_pos']
                end = partition.iloc[index + 1]['start_pos']
                part = text[start:end].strip()
                # print(index, start, end)
                # print(part)
                # print('<<',part)
                # print(start, end, partition.iloc[index]['part_1'], partition.iloc[index]['part_2'])
                # print(re.search(r'(https://)?(www\.)?[a-z\-.]+\.go\.id', part))
                # print('++', part)
                part = re.sub(' +', ' ',
                              re.sub('([^\w()][^\w()]){2,}', ' ',
                                     re.sub(r'^' + partition.iloc[index]['partition'] + r'[^\w(]*', '',
                                            re.sub(r'(?<=[a-z][a-z])\.\s*', '.\n',
                                                   re.sub(r'\n', ' ',
                                                          re.sub(
                                                              r'((https://)?(www\.)?[a-z\-.]+\.go\.id)|(\s\d{4},?\s*no\.\s*\d+\s+\d+)|(\s+\d+\s+\d{4},?\s*no\.\s*\d+)',
                                                              '',
                                                              part)))))).strip()
                # print('<<', part)
                # print(part)
                parts.append(part)
            except Exception:
                if partition.iloc[index]['part_1'] != 'lampiran':
                    start = partition.iloc[index]['start_pos']
                    # print(start, 'end', partition.iloc[index]['part_1'], partition.iloc[index]['part_2'])
                    # print('++', part)
                    part = text[start:].strip()
                    part = re.sub(' +', ' ',
                                  re.sub('([^\w()][^\w()]){2,}', ' ',
                                         re.sub(r'^' + partition.iloc[index]['partition'] + r'[\W\s]*', '',
                                                re.sub(r'(?<=[a-z][a-z])\.\s*|(?<=\(\d\d\))\.\s|(?<=\(\d\))\.\s*',
                                                       '.\n',
                                                       re.sub(r'\n', ' ',
                                                              re.sub(
                                                                  r'((https://)?(www\.)?[a-z\-.]+\.go\.id)|(\s\d{4},?\s*no\.\s*\d+\s+\d+)|(\s+\d+\s+\d{4},?\s*no\.\s*\d+)',
                                                                  '',
                                                                  part)))))).strip()
                    # print('>>', part)
                else:
                    part = ''
                parts.append(part)
            # print(part)
            # print()

        res = partition.assign(value=parts)
    #     # print(res)
    #     # res.to_csv('D:/Ninggar/Mgstr/Semester 2/Data/files/parsed_files/perda/2017/perwal_no_02_tahun_2017_petunjuk_teknis_pembiayaan_jaminan_kesehatan_masyarakat_miskin_di_luar_kuota_penerima_bantuan_iuran_jaminan_kesehatan_dan_bantuan_sosial_tidak_terencana_bagi_orang_terlantar.csv', index=False)
    #     res.to_csv(filetarget, index=False)
    #     file_num += 1
    #     # print(file_num, filesource)
    #     # break
    except Exception:
        failed.append(filesource)
        # state = False
        # print(Exception.with_traceback())
    print(filesource)
    break
print(failed)
print(len(failed))
file_list.close()
