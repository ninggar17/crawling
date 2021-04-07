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
while state:
    text = file_list.readline()
    # text='http://peraturan.go.id/common/dokumen/ln/1963/pp0271963.txt'
    print(line, text)
    line += 1
    if text == '':
        state = False
    text = text.strip()
    filesource = 'E:/Ninggar/Mgstr/Semester 2/Data/files/ayat_parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '.csv', re.sub('(%20)|([\\._]+)', '_',
                                                              re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                     '', text)))).group(1).lower()) + '.csv'
    filetarget = 'E:/Ninggar/Mgstr/Semester 2/Data/files/refine_ayat_parsed_files/' + (
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
        data = pandas.read_csv(filesource)
        # data = pandas.read_csv('D:/Ninggar/Mgstr/Semester 2/Data/files/ayat_parsed_files/perda/2018/2018-33.csv')
    except FileNotFoundError:
        print('not found')
        continue
    # data = pandas.read_csv(filesource)
    try:
        year = re.search('files/[^/]+/[^/]+/(\\d+)', filesource).group(1)
        stopword = r'(\n?' + year + r'\W+no\W+\d+\W+\d+(\W+|$)' + ')|(' + r'\n?\d+\W+' + year + r'\W+no\W+\d+(\W+|$))'
        before_pasal_pos = 0
        # gat max pos of header
        # print(data)
        for row in data.iterrows():
            # print(row[1]['part_1'], row[1]['part_2'], row[1]['start_pos'])
            if row[1]['part_1'] not in ('pasal', 'bab', 'diundangkan'):
                before_pasal_pos = max(before_pasal_pos, row[1]['start_pos'])
        # print(before_pasal_pos)
        # remove pasal between header
        before_pasal_item = {}
        new_data_array = []
        for row in data.iterrows():
            # print(row[1]['part_1'], row[1]['part_2'], row[1]['start_pos'])
            new_row = {}
            if row[1]['start_pos'] <= before_pasal_pos and row[1]['part_1'] not in ('pasal', 'bab', 'diundangkan'):
                before_pasal_item['partition'] = row[1]['partition']
                before_pasal_item['part_1'] = row[1]['part_1']
                before_pasal_item['part_2'] = row[1]['part_2']
                before_pasal_item['start_pos'] = row[1]['start_pos']
                before_pasal_item['ayat'] = row[1]['ayat']
                if 'nan' != str(row[1]['value']):
                    new_row['value'] = re.sub(stopword, ' ', row[1]['value'])
                else:
                    new_row['value'] = row[1]['value']
                new_row['partition'] = row[1]['partition']
                new_row['part_1'] = row[1]['part_1']
                new_row['part_2'] = row[1]['part_2']
                new_row['start_pos'] = row[1]['start_pos']
                new_row['ayat'] = row[1]['ayat']
                new_row['filename'] = row[1]['filename']
            elif len(before_pasal_item) > 0 and row[1]['start_pos'] <= before_pasal_pos and \
                    row[1]['part_1'] in ('pasal', 'bab', 'diundangkan'):
                # print('before', row[1]['part_1'], row[1]['part_2'], row[1]['start_pos'], row[1]['value'])
                # print(before_pasal_item)
                if str(row[1]['ayat']) != 'nan':
                    new_row['value'] = row[1]['partition'] + ' ' + str(row[1]['ayat']) + ' ' + re.sub(stopword, ' ',
                                                                                                      str(row[1]['value']))
                else:
                    new_row['value'] = row[1]['partition'] + ' ' + re.sub(stopword, ' ', row[1]['value'])
                new_row['partition'] = before_pasal_item['partition']
                new_row['part_1'] = before_pasal_item['part_1']
                new_row['part_2'] = before_pasal_item['part_2']
                new_row['start_pos'] = before_pasal_item['start_pos']
                new_row['ayat'] = before_pasal_item['ayat']
                new_row['filename'] = row[1]['filename']
            elif len(before_pasal_item) == 0 and row[1]['start_pos'] <= before_pasal_pos and \
                    row[1]['part_1'] in ('pasal', 'bab', 'diundangkan'):
                continue
            else:
                try:
                    new_row['value'] = re.sub(stopword, ' ', row[1]['value'])
                except TypeError:
                    new_row['value'] = row[1]['value']
                new_row['partition'] = row[1]['partition']
                new_row['part_1'] = row[1]['part_1']
                new_row['part_2'] = row[1]['part_2']
                new_row['start_pos'] = row[1]['start_pos']
                new_row['ayat'] = row[1]['ayat']
                new_row['filename'] = row[1]['filename']
            new_data_array.append(new_row)
        data = pandas.DataFrame(new_data_array)
        new_data_array = []
        last_state = {}
        for row in data.iterrows():
            if len(last_state) == 0:
                last_state['partition'] = row[1]['partition']
                last_state['part_1'] = row[1]['part_1']
                last_state['part_2'] = row[1]['part_2']
                last_state['start_pos'] = row[1]['start_pos']
                last_state['ayat'] = row[1]['ayat']
                last_state['filename'] = row[1]['filename']
                last_state['value'] = row[1]['value']
            elif last_state['partition'] == row[1]['partition'] and last_state['part_1'] == row[1]['part_1'] and \
                    str(last_state['part_2']) == str(row[1]['part_2']) and last_state['start_pos'] == row[1][
                'start_pos'] and \
                    str(last_state['ayat']) == str(row[1]['ayat']) and last_state['filename'] == row[1]['filename']:
                if str(last_state['value']) == 'nan':
                    last_state['value'] = row[1]['value']
                else:
                    last_state['value'] += ' ' + str(row[1]['value'])
            else:
                new_data_array.append(last_state)
                last_state = {'partition': row[1]['partition'], 'part_1': row[1]['part_1'], 'part_2': row[1]['part_2'],
                              'start_pos': row[1]['start_pos'], 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                              'value': row[1]['value']}
        new_data_array.append(last_state)
        data = pandas.DataFrame(new_data_array)
        # print(data)
        new_data_array = []
        for row in data.iterrows():
            # print(row[1]['part_1'])
            if row[1]['part_1'] in ('memutuskan', 'menetapkan', 'pasal', 'bab', 'diundangkan'):
                try:
                    value_array = re.split('\\.\\s+pasal\\s+\\d+\\s*', row[1]['value'])
                    new_pasal_array = re.findall('\\.\\s+(pasal\\s+\\d+)\\s*', row[1]['value'])
                    # print(json.dumps(value_array, indent=4))
                    # print(new_pasal_array)
                except TypeError:
                    value_array = []
                    new_pasal_array = []
                if len(value_array) > 1:
                    new_data_array.append(
                        {'partition': row[1]['partition'], 'part_1': row[1]['part_1'], 'part_2': row[1]['part_2'],
                         'start_pos': row[1]['start_pos'], 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                         'value': value_array[0] + '.'})
                    for part_index in range(1, len(value_array)):
                        parts = re.split(' ', re.sub('\\s+', ' ', new_pasal_array[part_index - 1]))
                        if part_index < len(value_array) - 1:
                            value_array[part_index] += '.'
                        new_data_array.append(
                            {'partition': new_pasal_array[part_index - 1], 'part_1': parts[0],
                             'part_2': parts[1], 'start_pos': row[1]['start_pos'], 'ayat': None,
                             'filename': row[1]['filename'],
                             'value': value_array[part_index]})
                else:
                    new_data_array.append(
                        {'partition': row[1]['partition'], 'part_1': row[1]['part_1'], 'part_2': row[1]['part_2'],
                         'start_pos': row[1]['start_pos'], 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                         'value': row[1]['value']})
            else:
                new_data_array.append(
                    {'partition': row[1]['partition'], 'part_1': row[1]['part_1'], 'part_2': row[1]['part_2'],
                     'start_pos': row[1]['start_pos'], 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                     'value': row[1]['value']})
        data = pandas.DataFrame(new_data_array)
        new_data_array = []
        skip = False
        # print(data['ayat'])

        for row_index in range(len(data)):
            if skip:
                skip = False
                continue
            # print(data.iloc[row_index]['value'])
            # print(str(data.iloc[row_index]['ayat']) != 'nan' and int(data.iloc[row_index]['ayat']) == 1 and \
            #         data.iloc[row_index - 1]['part_1'] in ('pasal', 'bab'))
            if row_index != len(data) - 1 and str(data.iloc[row_index + 1]['ayat']) in ('1', '1.0') and \
                    data.iloc[row_index]['part_1'] == 'pasal' and data.iloc[row_index]['value'] == '' and \
                    str(data.iloc[row_index - 1]['ayat']) not in ('1', '1.0'):
                new_data_array.append(
                    {'partition': data.iloc[row_index]['partition'], 'part_1': data.iloc[row_index]['part_1'],
                     'part_2': data.iloc[row_index]['part_2'], 'start_pos': data.iloc[row_index + 1]['start_pos'],
                     'ayat': data.iloc[row_index + 1]['ayat'], 'filename': data.iloc[row_index]['filename'],
                     'value': data.iloc[row_index + 1]['value']}
                )
                skip = True
            elif 'nan' not in (str(data.iloc[row_index - 1]['ayat']), str(data.iloc[row_index]['ayat'])) and \
                    int(data.iloc[row_index]['ayat']) == int(data.iloc[row_index - 1]['ayat']) + 1:
                new_data_array.append(
                    {'partition': new_data_array[len(new_data_array) - 1]['partition'],
                     'part_1': new_data_array[len(new_data_array) - 1]['part_1'],
                     'part_2': new_data_array[len(new_data_array) - 1]['part_2'],
                     'start_pos': data.iloc[row_index]['start_pos'],
                     'ayat': data.iloc[row_index]['ayat'],
                     'filename': data.iloc[row_index]['filename'],
                     'value': data.iloc[row_index]['value']}
                )
            elif str(data.iloc[row_index]['ayat']) != 'nan' and int(data.iloc[row_index]['ayat']) == 1 and \
                    data.iloc[row_index - 1]['part_1'] in ('pasal', 'bab'):
                # print(new_data_array)
                if new_data_array[len(new_data_array) - 1]['part_1'] == 'pasal' and \
                        str(new_data_array[len(new_data_array) - 1]['part_2']) != 'nan':
                    new_data_array.append(
                        {'partition': 'pasal ' + str(int(new_data_array[len(new_data_array) - 1]['part_2']) + 1),
                         'part_1': 'pasal',
                         'part_2': int(new_data_array[len(new_data_array) - 1]['part_2']) + 1,
                         'start_pos': data.iloc[row_index]['start_pos'],
                         'ayat': data.iloc[row_index]['ayat'],
                         'filename': data.iloc[row_index]['filename'],
                         'value': data.iloc[row_index]['value']}
                    )
                elif data.iloc[row_index - 2]['part_1'] == 'pasal' and \
                        str(new_data_array[len(new_data_array) - 2]['part_2']) != 'nan':
                    new_data_array.append(
                        {'partition': 'pasal ' + str(int(new_data_array[len(new_data_array) - 2]['part_2']) + 1),
                         'part_1': 'pasal',
                         'part_2': int(new_data_array[len(new_data_array) - 2]['part_2']) + 1,
                         'start_pos': data.iloc[row_index]['start_pos'],
                         'ayat': data.iloc[row_index]['ayat'],
                         'filename': data.iloc[row_index]['filename'],
                         'value': data.iloc[row_index]['value']}
                    )
                else:
                    new_data_array.append(
                        {'partition': data.iloc[row_index]['partition'], 'part_1': data.iloc[row_index]['part_1'],
                         'part_2': data.iloc[row_index]['part_2'], 'start_pos': data.iloc[row_index]['start_pos'],
                         'ayat': data.iloc[row_index]['ayat'], 'filename': data.iloc[row_index]['filename'],
                         'value': data.iloc[row_index]['value']}
                    )
            else:
                new_data_array.append(
                    {'partition': data.iloc[row_index]['partition'], 'part_1': data.iloc[row_index]['part_1'],
                     'part_2': data.iloc[row_index]['part_2'], 'start_pos': data.iloc[row_index]['start_pos'],
                     'ayat': data.iloc[row_index]['ayat'], 'filename': data.iloc[row_index]['filename'],
                     'value': data.iloc[row_index]['value']}
                )
        # print(new_data_array)
        data = pandas.DataFrame(new_data_array)
        data.to_csv(filetarget)
    except Exception:
        failed.append(filesource)
        # state = False
        # print(Exception.with_traceback())
    # break
print(json.dumps(failed, indent=4))
    # new_data_array = []
    # for row_index in range(len(data)):
    #     print(data.iloc[row_index]['start_pos'], data.iloc[row_index]['part_1'], data.iloc[row_index]['part_2'],
    #           data.iloc[row_index]['ayat'], data.iloc[row_index]['value'])
