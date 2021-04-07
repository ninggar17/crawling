import re

import numpy
import pandas

file_list = open('C:/Users/Bukalapak/PycharmProjects/crawling/crawl/pdf_list', 'r+')
file_num = 0
state = True
res = None
while state:
    text = file_list.readline()
    if text == '':
        state = False
    text = text.strip()
    filesource = 'D:/Ninggar/Mgstr/Semester 2/Data/files/text_files/' + (
        re.sub('_pdf', '.txt', re.sub('(%20)|([\\._]+)', '_',
                                      re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
    file_name = filesource
    try:
        file = open(file_name)
    except Exception:
        continue
    if re.search('putusan', filesource):
        continue
    text = file.read()
    text = (re.sub('\\nwww.djpp.depkumham.go.id\\n', '\n',
                   re.sub('[^a-zA-Z\\d\\n./:(&-,);]', ' ',
                          re.sub('\\n+', '\n', re.sub('\\n\\s+\\n', '\\n\\n', text))))).lower().strip()
    parse = re.finditer('((tentang)|(menimbang)|(bagian)|(paragraf)|(mengingat)|(memutuskan)|(menetapkan)|(lampiran)|(diundangkan)|(bab[ ]+([ivxlcdm]+|\\d+))|(pasal[ ]+\\d+))', text)
    valid = True
    partition = {}
    while valid:
        try:
            text_parse = parse.__next__()
        except Exception:
            break
        try:
            partition[text_parse.group(0).strip()] = min(partition[text_parse.group(0).strip()], text_parse.start())
        except Exception:
            partition[text_parse.group(0).strip()] = text_parse.start()
    x = numpy.array([list(partition.keys()), list(partition.values())]).transpose()
    df = pandas.DataFrame(x, columns=['partition', 'start_pos'])
    df['filename'] = file_name
    if res is None:
        res = df
    else:
        res = pandas.concat([res, df])
    # print(df)
    file.close()
    file_num += 1
    print(file_num, filesource)
file_list.close()
res.drop_duplicates(subset=None, keep='first', inplace=False)
res.to_csv('files_partition_tag.csv')
# file_name = 'D:/Ninggar/Mgstr/Semester 2/Data/files/text_files/bn/2007/bn1-2007.txt'
# file = open(file_name)
#
# text = file.read()
# text = (re.sub('\\nwww.djpp.depkumham.go.id\\n', '\n',
#                re.sub('[^a-zA-Z\\d\\n./:(&-,);]', ' ',
#                       re.sub('\\n+', '\n', re.sub('\\n\\s+\\n', '\\n\\n', text))))).lower().strip()
# parse = re.finditer('((^.+)|(tentang)|(menimbang)|(mengingat)|(memutuskan)|(bab.*))', text)
# valid = True
# partition = {}
# while valid:
#     try:
#         text_parse = parse.__next__()
#     except Exception:
#         break
#     try:
#         partition[text_parse.group(0).strip()] = min(partition[text_parse.group(0).strip()], text_parse.start())
#     except Exception:
#         partition[text_parse.group(0).strip()] = text_parse.start()
# x = numpy.array([list(partition.keys()), list(partition.values())]).transpose()
# df = pandas.DataFrame(x, columns=['partition', 'start_pos'])
# df['filename'] = file_name
# print(df)
# file.close()
