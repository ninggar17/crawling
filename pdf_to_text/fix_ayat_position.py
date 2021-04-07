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
    filesource = 'D:/Ninggar/Mgstr/Semester 2/Data/files/refine_ayat_parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '.csv', re.sub('(%20)|([\\._]+)', '_',
                                                              re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                     '', text)))).group(1).lower()) + '.csv'
    filetarget = 'D:/Ninggar/Mgstr/Semester 2/Data/files/fix_position_v1/' + (
        re.search(r'(.{,100})', re.sub('(%20)|([\\._]+)', '_', re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                      '', text)))).group(1).lower() + '.csv'
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
    # print(data.dtypes)
    try:
        pasals = [[]] * int(data.part_2.max())
        for row in data.iterrows():
            cols = row[1][['partition', 'part_1', 'part_2', 'ayat', 'value']]
            # print('cols', cols['value'])
            if cols['part_1'] == 'pasal':
                ayat_value = {'ayat': cols['ayat'], 'value': re.split('\n', cols['value'])}
                if len(pasals[int(cols['part_2']) - 1]) == 0:
                    pasals[int(cols['part_2']) - 1] = [{}] * (int(data.ayat.max()) + 1)
                try:
                    # print(int(cols['part_2']), int(cols['ayat']))
                    temp = pasals[int(cols['part_2']) - 1].copy()
                    temp[int(cols['ayat'])] = ayat_value
                    pasals[int(cols['part_2']) - 1] = temp
                    # print(new_pasal[int(cols['part_2']) - 1][int(cols['ayat'])]['value'])
                except ValueError:
                    # print(int(cols['part_2']), str(0))
                    temp = pasals[int(cols['part_2']) - 1].copy()
                    temp[0] = ayat_value
                    pasals[int(cols['part_2']) - 1] = temp
                    # print(new_pasal[int(cols['part_2']) - 1][0]['value'])
                # break
        # print(json.dumps(pasals, indent=4))
        # for row in data.iterrows():
        #     print(str(row[1]['part_2']), str(row[1]['ayat']))
        for idx_pasal in range(len(pasals)):
            # print(idx_pasal)
            if not pasals[idx_pasal] and 0 < idx_pasal < (len(pasals) - 1):
                max_not_null = None
                for ayat_bef in range(len(pasals[idx_pasal - 1])):
                    if pasals[idx_pasal - 1][ayat_bef]:
                        max_not_null = ayat_bef
                if (max_not_null is None or len(pasals[idx_pasal - 1][max_not_null]['value']) != 2) \
                        and pasals[idx_pasal + 1] and pasals[idx_pasal + 1][0] \
                        and len(pasals[idx_pasal + 1][0]['value']) == 2:
                    ayat_value = {'ayat': pasals[idx_pasal + 1][0]['ayat'], 'value': [pasals[idx_pasal + 1][0]['value'][0]]}
                    temp = [{}] * (int(data.ayat.max()) + 1)
                    temp[0] = ayat_value
                    pasals[idx_pasal] = temp
                    false_ayat = {'ayat': pasals[idx_pasal + 1][0]['ayat'], 'value': [pasals[idx_pasal + 1][0]['value'][1]]}
                    temp1 = pasals[idx_pasal + 1].copy()
                    temp1[0] = false_ayat
                    pasals[idx_pasal + 1] = temp1
                elif max_not_null is not None and len(pasals[idx_pasal - 1][max_not_null]['value']) == 2:
                    ayat_value = {'ayat': None,
                                  'value': [pasals[idx_pasal - 1][max_not_null]['value'][1]]}
                    temp = [{}] * (int(data.ayat.max()) + 1)
                    temp[0] = ayat_value
                    pasals[idx_pasal] = temp
                    false_ayat = {'ayat': pasals[idx_pasal - 1][max_not_null]['ayat'],
                                  'value': [pasals[idx_pasal - 1][max_not_null]['value'][0]]}
                    temp1 = pasals[idx_pasal - 1].copy()
                    temp1[max_not_null] = false_ayat
                    pasals[idx_pasal - 1] = temp1
            if pasals[idx_pasal]:
                for idx_ayat in range(len(pasals[idx_pasal])):
                    if not pasals[idx_pasal][idx_ayat]:
                        if not pasals[idx_pasal][0] \
                                and idx_ayat == 1 \
                                and pasals[idx_pasal][2] \
                                and 0 < idx_pasal \
                                and pasals[idx_pasal - 1][0] \
                                and pasals[idx_pasal - 1][1]:
                            ayat_value = pasals[idx_pasal - 1][1]
                            temp = pasals[idx_pasal].copy()
                            temp[0] = ayat_value
                            pasals[idx_pasal] = temp
                            false_ayat = {}
                            temp1 = pasals[idx_pasal - 1].copy()
                            temp1[1] = false_ayat
                            pasals[idx_pasal - 1] = temp1
                        if 0 < idx_ayat < int(data.ayat.max()) \
                                and len(pasals[idx_pasal][idx_ayat + 1]) >= 1 \
                                and len(pasals[idx_pasal][idx_ayat - 1]) == 1:
                            ayat_value = pasals[idx_pasal][idx_ayat + 1]
                            ayat_value['ayat'] -= 1
                            ayat_value['value'] = ayat_value['value'][:len(ayat_value['value']) - 1]
                            temp = pasals[idx_pasal].copy()
                            temp[0] = ayat_value
                            pasals[idx_pasal] = temp
                            false_ayat = pasals[idx_pasal][idx_ayat + 1]
                            false_ayat['value'] = false_ayat['value'][len(false_ayat['value']) - 1:]
                            temp1 = pasals[idx_pasal + 1].copy()
                            temp1[1] = false_ayat
                            pasals[idx_pasal + 1] = temp1
                        if 0 < idx_ayat < int(data.ayat.max()) \
                                and len(pasals[idx_pasal][idx_ayat - 1]) >= 1 \
                                and len(pasals[idx_pasal][idx_ayat + 1]) == 1:
                            ayat_value = pasals[idx_pasal][idx_ayat - 1]
                            ayat_value['ayat'] += 1
                            ayat_value['value'] = ayat_value['value'][len(ayat_value['value']) - 1:]
                            temp = pasals[idx_pasal].copy()
                            temp[0] = ayat_value
                            pasals[idx_pasal] = temp
                            false_ayat = pasals[idx_pasal][idx_ayat - 1]
                            false_ayat['value'] = false_ayat['value'][:len(false_ayat['value']) - 1]
                            temp1 = pasals[idx_pasal - 1].copy()
                            temp1[1] = false_ayat
                            pasals[idx_pasal - 1] = temp1

        # print(json.dumps(pasals, indent=4))
        new_data = []
        pasal_handled = 0
        for row in data.iterrows():
            # print(row[1]['part_1'], str(row[1]['part_2']), pasal_handled)
            if row[1]['part_1'] == 'pasal' and pasal_handled < int(row[1]['part_2']):
                for idx_pasal in range(pasal_handled, int(row[1]['part_2'])):
                    for ayat in pasals[idx_pasal]:
                        if ayat:
                            # if len(ayat['value']) > 1:
                            # print(ayat['ayat'], len(ayat['value']))
                            # print(json.dumps(ayat['value'], indent=4))
                            new_data.append({'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                                             'part_2': idx_pasal + 1, 'start_pos': row[1]['start_pos'],
                                             'ayat': ayat['ayat'], 'filename': row[1]['filename'],
                                             'value': ayat['value']})
                    pasal_handled += 1
            elif row[1]['part_1'] != 'pasal':
                new_data.append({'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                                 'part_2': row[1]['part_2'], 'start_pos': row[1]['start_pos'],
                                 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                 'value': row[1]['value']})
        data = pandas.DataFrame(new_data)
        # print(data)
        new_value = []
        # print('bbb')
        other_row = []
        for row in data.iterrows():
            # print(row[1]['part_1'], str(row[1]['part_2']))
            # print(json.dumps(row[1]['value'], indent=4))
            if row[1]['part_1'] == 'pasal' and len(row[1]['value']) > 1:
                idx_root = -1
                text_merge = []
                for idx_v in range(len(row[1]['value'])):
                    if re.search(r'^((\w[^\w\s])|(\d+[^\w\s]))', row[1]['value'][idx_v]):
                        if idx_root == -1:
                            row[1]['value'][idx_v - 1] += ' ' + row[1]['value'][idx_v]
                            idx_root = idx_v - 1
                        else:
                            row[1]['value'][idx_root] += ' ' + row[1]['value'][idx_v]
                        text_merge.append(row[1]['value'][idx_v])
                    elif not re.match(r'^.+\.$', row[1]['value'][idx_v]):
                        other = row[1].copy()
                        other['value'] = [row[1]['value'][idx_v]]
                        other_row.append(other)
                        text_merge.append(row[1]['value'][idx_v])
                    else:
                        idx_root = -1
                new_value.append([x for x in row[1]['value'] if x not in text_merge])
            else:
                new_value.append(row[1]['value'])
        data['value'] = new_value
        new_data = []
        other_row_idx = 0
        for idx_row in range(len(data)):
            # print(other_row_idx, len(other_row), other_row_idx < len(other_row))
            if other_row_idx < len(other_row) \
                    and data.iloc[idx_row]['partition'] == other_row[other_row_idx]['partition'] \
                    and data.iloc[idx_row]['part_1'] == other_row[other_row_idx]['part_1'] \
                    and data.iloc[idx_row]['part_2'] == other_row[other_row_idx]['part_2'] \
                    and str(data.iloc[idx_row]['ayat']) == str(other_row[other_row_idx]['ayat']):
                new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': data.iloc[idx_row]['part_1'],
                                 'part_2': data.iloc[idx_row]['part_2'], 'start_pos': data.iloc[idx_row]['start_pos'],
                                 'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                 'value': data.iloc[idx_row]['value']})
                new_data.append(
                    {'partition': other_row[other_row_idx]['partition'], 'part_1': 'other',
                     'part_2': -1, 'start_pos': other_row[other_row_idx]['start_pos'],
                     'ayat': -1, 'filename': other_row[other_row_idx]['filename'],
                     'value': other_row[other_row_idx]['value']})
                other_row_idx += 1
            else:
                new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': data.iloc[idx_row]['part_1'],
                                 'part_2': data.iloc[idx_row]['part_2'], 'start_pos': data.iloc[idx_row]['start_pos'],
                                 'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                 'value': data.iloc[idx_row]['value']})
        data = pandas.DataFrame(new_data)
        # print('sss')
        # print(other_row)
        double_row = 0
        new_data = []
        for row in data.iterrows():
            # print(row[1]['part_1'], str(row[1]['part_2']), double_row, row[1]['part_2'] + double_row)
            if row[1]['part_1'] == 'pasal' and len(row[1]['value']) > 1:
                # print(row[1]['part_1'], str(row[1]['part_2']), row[1]['ayat'])
                # print(json.dumps(row[1]['value'], indent=4))
                for idx_v in range(len(row[1]['value'])):
                    if idx_v != 0:
                        row[1]['ayat'] = None
                    new_data.append({'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                                     'part_2': row[1]['part_2'] + double_row, 'start_pos': row[1]['start_pos'],
                                     'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                     'value': [row[1]['value'][idx_v]]})
                    if idx_v < len(row[1]['value']) - 1:
                        double_row += 1
            elif row[1]['part_1'] == 'pasal':
                new_data.append({'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                                 'part_2': row[1]['part_2'] + double_row, 'start_pos': row[1]['start_pos'],
                                 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                 'value': row[1]['value']})
            else:
                new_data.append({'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                                 'part_2': row[1]['part_2'], 'start_pos': row[1]['start_pos'],
                                 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                 'value': row[1]['value']})
        # print(filetarget)
        data = pandas.DataFrame(new_data)
        new_data = []
        for row in data.iterrows():
            if row[1]['part_1'] in ('bab', 'other'):
                if row[1]['part_1'] == 'other':
                    row[1]['value'] = row[1]['value'][0]
                # print(row[1]['part_1'], str(row[1]['part_2']), row[1]['value'])
                row[1]['value'] = re.sub(r'pasal \d+', '', row[1]['value'])
                parts = re.findall(
                    r'bagian\ske[\w]+|bagian\s[\w]+|paragraf\s\d+|bab\s[ivxlcdm]+|bab\s\\d+',
                    row[1]['value']
                )
                values = re.split(r'bagian\ske[\w]+|bagian\s[\w]+|paragraf\s\d+|bab\s[ivxlcdm]+|bab\s\\d+', row[1]['value'])
                values = [x.strip() for x in values if x.strip() != '']
                map = []
                if len(parts) < len(values):
                    for value_idx in range(len(values) - len(parts)):
                        new_data.append({'partition': row[1]['partition'], 'part_1': None,
                                         'part_2': None, 'start_pos': row[1]['start_pos'],
                                         'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                         'value': values[value_idx]})
                    for part_idx in range(len(parts)):
                        parts_split = re.split(r'\s', parts[part_idx])
                        if len(parts_split) == 2:
                            part_1 = parts_split[0]
                            try:
                                part_2 = int(parts_split[1])
                            except ValueError:
                                try:
                                    part_2 = roman_to_int(parts_split[1])
                                except KeyError:
                                    part_2 = latin_to_int(re.sub('^ke', '', parts_split[1]))
                            new_data.append({'partition': row[1]['partition'], 'part_1': part_1,
                                             'part_2': part_2, 'start_pos': row[1]['start_pos'],
                                             'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                             'value': values[part_idx + 1]})
                elif len(parts) == len(values):
                    for part_idx in range(len(parts)):
                        parts_split = re.split(r'\s', parts[part_idx])
                        if len(parts_split) == 2:
                            part_1 = parts_split[0]
                            try:
                                part_2 = int(parts_split[1])
                            except ValueError:
                                try:
                                    part_2 = roman_to_int(parts_split[1])
                                except KeyError:
                                    part_2 = latin_to_int(re.sub('^ke', '', parts_split[1]))
                            new_data.append({'partition': row[1]['partition'], 'part_1': part_1,
                                             'part_2': part_2, 'start_pos': row[1]['start_pos'],
                                             'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                             'value': values[part_idx]})
            else:
                new_data.append({'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                                 'part_2': row[1]['part_2'], 'start_pos': row[1]['start_pos'],
                                 'ayat': row[1]['ayat'], 'filename': row[1]['filename'],
                                 'value': row[1]['value']})
        data = pandas.DataFrame(new_data)
        new_data = []
        last_bab = 0
        for idx_row in range(len(data)):
            if data.iloc[idx_row]['part_1'] == 'bab':
                last_bab = data.iloc[idx_row]['part_2']
                new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': data.iloc[idx_row]['part_1'],
                                 'part_2': data.iloc[idx_row]['part_2'], 'start_pos': data.iloc[idx_row]['start_pos'],
                                 'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                 'value': data.iloc[idx_row]['value']})
            elif data.iloc[idx_row]['part_1'] is None and \
                    re.search('bab', data.iloc[idx_row + 1]['partition']) and \
                    data.iloc[idx_row + 1]['part_1'] != 'bab' and \
                    not re.search('bab', data.iloc[idx_row - 1]['partition']):
                parts_split = re.split(r'\s', data.iloc[idx_row + 1]['partition'])
                if len(parts_split) == 2:
                    part_1 = parts_split[0]
                    try:
                        part_2 = int(parts_split[1])
                    except ValueError:
                        try:
                            part_2 = roman_to_int(parts_split[1])
                        except KeyError:
                            part_2 = latin_to_int(re.sub('^ke', '', parts_split[1]))
                    last_bab = part_2
                    new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': part_1,
                                     'part_2': part_2, 'start_pos': data.iloc[idx_row]['start_pos'],
                                     'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                     'value': data.iloc[idx_row]['value']})
            elif data.iloc[idx_row]['part_1'] is None and \
                    re.search('bab', data.iloc[idx_row - 1]['partition']) and \
                    data.iloc[idx_row - 1]['part_1'] != 'bab' and \
                    not re.search('bab', data.iloc[idx_row + 1]['partition']):
                parts_split = re.split(r'\s', data.iloc[idx_row - 1]['partition'])
                if len(parts_split) == 2:
                    part_1 = parts_split[0]
                    try:
                        part_2 = int(parts_split[1])
                    except ValueError:
                        try:
                            part_2 = roman_to_int(parts_split[1])
                        except KeyError:
                            part_2 = latin_to_int(re.sub('^ke', '', parts_split[1]))
                    last_bab = part_2
                    new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': part_1,
                                     'part_2': part_2, 'start_pos': data.iloc[idx_row]['start_pos'],
                                     'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                     'value': data.iloc[idx_row]['value']})
            elif data.iloc[idx_row]['part_1'] is None:
                last_bab += 1
                new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': 'bab',
                                 'part_2': last_bab, 'start_pos': data.iloc[idx_row]['start_pos'],
                                 'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                 'value': data.iloc[idx_row]['value']})
            else:
                new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': data.iloc[idx_row]['part_1'],
                                 'part_2': data.iloc[idx_row]['part_2'], 'start_pos': data.iloc[idx_row]['start_pos'],
                                 'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                 'value': data.iloc[idx_row]['value']})
        # data.to_csv('tryres.csv')
        data = pandas.DataFrame(new_data)
        new_data = []
        last_pasal = 0
        for idx_row in range(len(data)):
            if data.iloc[idx_row]['part_1'] == 'pasal':
                if last_pasal + 1 < data.iloc[idx_row]['part_2']:
                    new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': 'other',
                                     'part_2': '-1', 'start_pos': data.iloc[idx_row]['start_pos'],
                                     'ayat': '-1', 'filename': data.iloc[idx_row]['filename'],
                                     'value': data.iloc[idx_row]['value']})
                else:
                    new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': data.iloc[idx_row]['part_1'],
                                     'part_2': data.iloc[idx_row]['part_2'], 'start_pos': data.iloc[idx_row]['start_pos'],
                                     'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                     'value': data.iloc[idx_row]['value']})
                    last_pasal = data.iloc[idx_row]['part_2']
            else:
                new_data.append({'partition': data.iloc[idx_row]['partition'], 'part_1': data.iloc[idx_row]['part_1'],
                                 'part_2': data.iloc[idx_row]['part_2'], 'start_pos': data.iloc[idx_row]['start_pos'],
                                 'ayat': data.iloc[idx_row]['ayat'], 'filename': data.iloc[idx_row]['filename'],
                                 'value': data.iloc[idx_row]['value']})
        data = pandas.DataFrame(new_data)
        data.to_csv(filetarget)
    except Exception:
        failed.append(filesource)
        # state = False
        # print(Exception.with_traceback())
print(json.dumps(failed, indent=4))
    # break
