import json
import os
import re

import pandas
import stanza


def get_title(texts):
    try:
        filtered_text = re.search(
            r'((PERATURAN|UNDANG\-UNDANG|KEPUTUSAN)[A-Z0-9\W\s\n]+(DENGAN\sRAHMAT\sTUHAN\sYANG\sMAHA\sESA)?[A-Z0-9\W\s\n]+(,?))[\s\n]*Menimbang',
            texts).group(1)
    except Exception:
        return ''
    # print(filtered_text)
    res = {}
    try:
        res = {'type': re.sub(r'[\n\s.]+', ' ', re.search(r'^([A-Z/,.\-\s\n]+)NOMOR', filtered_text).group(1).strip()),
               'number': re.sub(r'[\n\s]+', ' ', re.search(r'NOMOR[\n\s]*((:|[A-Z]+[\s\n]?[-.]?|[^/\d]+)[\n\s]*)?(\d+)',
                                                           filtered_text).group(3).strip()),
               }
    except Exception:
        pass
    try:
        res['year'] = re.sub(r'[\n\s]+', ' ',
                             re.search(r'(TAHUN|/)[\n\s]*(\d+)[\s\n.X]*TENTANG', filtered_text).group(2).strip())
    except Exception:
        pass
    try:
        res['official'] = re.search(
            r'\n\s*((PJS|PRESIDEN|MENTERI|WALI KOTA|GUBERNUR|LEMBAGA|KEPALA|KETUA|DEWAN|BADAN|JAKSA)[A-Z/,\s\n]+),?[\s\n]+$',
            filtered_text).group(1).strip()
    except Exception:
        res['official'] = ''
    try:
        res['name'] = re.sub('DENGAN RAHMAT TUHAN YANG MAHA ESA', '', re.sub(r'[\n\s]+', ' ',
                                                                             re.search(r'TENTANG([\w\W\n\s]+)' + res[
                                                                                 'official'], filtered_text).group(
                                                                                 1).strip())).strip()
        res['official'] = re.sub(r'[\n\s]+', ' ', res['official']).strip()
    except Exception:
        pass
    # print(r'TENTANG([\w\W\n\s]+)((DENGAN\sRAHMAT\sTUHAN\sYANG\sMAHA\sESA[\s\n]+)?' + res['official']+')')
    # print(json.dumps(res, indent=4))
    return res


def get_considerans(texts):
    try:
        filtered_text = re.search(r'Menimbang\s+:([\w\W\n]+)Mengingat', texts).group(1).strip()
    except Exception:
        return []
    res = re.split(';', filtered_text)
    for idx in range(len(res)):
        res[idx] = re.sub(r'[\n\s]+', ' ', re.sub(r'([\s\n]+|^)[a-z]\.', ' ', res[idx])).strip()
    try:
        res.remove('')
    except Exception:
        pass
    return res


def get_law_based(texts):
    try:
        filtered_text = re.search(r'Mengingat\s+:([\w\W\n]+)MEMUTUSKAN', texts).group(1).strip()
    except Exception:
        return []
    res = re.split(';', filtered_text)
    for idx in range(len(res)):
        res[idx] = re.sub(r'[\n\s]+', ' ', re.sub(r'([\s\n]+|^)\d\.', ' ', res[idx])).strip()
    try:
        res.remove('')
    except Exception:
        pass
    return res


def get_dictum(texts):
    try:
        filtered_text = re.sub(r'[\n\s]+', ' ',
                               re.search(r'(MEMUTUSKAN[\s\n]*:)?([\s\n]*(Menetapkan|MENETAPKAN)[^.]+\.)', texts).group(
                                   2)).strip()
    except Exception:
        return ''
    return filtered_text


def get_body_part(texts):
    texts = re.sub(r'\n\s*(Agar\s*)?setiap\s*orang\s*(yang\s*)?(dapat\s*)?mengetahui(nya)?[\w\W\n]+', '', texts).strip()
    # print('jjj',texts)
    filtered_chapters = re.finditer(r'\n\s*(BAB[\s\n]*[IVXLCDM]+)\s*\n', texts)
    chapters = {}
    temp = None
    # print(texts)
    for chapter in filtered_chapters:
        if len(chapters) > 0:
            chapters[temp]['end'] = chapter.start(1)
        chapters[chapter.group(1)] = {'start': chapter.start(1)}
        temp = chapter.group(1)
    for key in chapters.keys():
        try:
            chapter_text = texts[chapters[key]['start']:chapters[key]['end']]
        except Exception:
            chapter_text = texts[chapters[key]['start']:]
        filtered_part = re.finditer(
            r'\n\s*(Bagian[\s\n]*[Kk]e(se(puluh|belas|ratus|mbilan)|satu|dua|tiga|empat|lima|enam|tujuh|delapan)[ a-zA-Z]*)\s*\n',
            chapter_text)
        # print(key)
        chapters[key]['parts'] = {}
        for part in filtered_part:
            if len(chapters[key]['parts']) > 0:
                chapters[key]['parts'][temp]['end'] = part.start(1)
            chapters[key]['parts'][part.group(1)] = {'start': part.start(1)}
            temp = part.group(1)
        for part_key in chapters[key]['parts'].keys():
            try:
                part_text = chapter_text[
                            chapters[key]['parts'][part_key]['start']:chapters[key]['parts'][part_key]['end']]
            except Exception:
                part_text = chapter_text[chapters[key]['parts'][part_key]['start']:]
            # print(part_text)
            filtered_pgph = re.finditer(r'\n\s*(Paragraf[\s\n]*\d+)\s*\n', part_text)
            chapters[key]['parts'][part_key]['paragraphs'] = {}
            for pgph in filtered_pgph:
                if len(chapters[key]['parts'][part_key]['paragraphs']) > 0:
                    chapters[key]['parts'][part_key]['paragraphs'][temp]['end'] = pgph.start(1)
                chapters[key]['parts'][part_key]['paragraphs'][pgph.group(1)] = {'start': pgph.start(1)}
                temp = pgph.group(1)
            if len(chapters[key]['parts'][part_key]['paragraphs']) > 0:
                for pgph_key in chapters[key]['parts'][part_key]['paragraphs'].keys():
                    try:
                        pgph_text = part_text[chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['start']:
                                              chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['end']]
                    except Exception:
                        pgph_text = part_text[chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['start']:]
                    chapters[key]['parts'][part_key]['paragraphs'][pgph_key]['articles'] = get_article('Paragraf',
                                                                                                       pgph_text)
            else:
                chapters[key]['parts'][part_key]['articles'] = get_article('Bagian', part_text)

        if len(chapters[key]['parts']) == 0:
            chapters[key]['articles'] = get_article('BAB', chapter_text.strip())

    if len(chapters) == 0:
        chapters['BAB 0'] = {'start': 0, 'parts': {}}
        chapters['BAB 0']['articles'] = get_article('', texts)
    # print(json.dumps(chapters, indent=4))
    # print(text)
    chapters = get_body_values(chapters, texts, 'chapters')
    return chapters


def get_article(part_name, texts):
    temp = None
    pattern = r'(?s)(^{part_name}[\w\W\n]*?|\.[\n\s]*)\n\s*(Pasal[\s\n]*(\d+|[IVXLCDM]+))\s*\n'
    filtered_pasal = re.finditer(pattern.format(part_name=part_name), texts)
    # print('jjj',texts)
    res = {}
    for pasal in filtered_pasal:
        if len(res) > 0:
            res[temp]['end'] = pasal.start(2)
        res[pasal.group(2)] = {'start': pasal.start(2)}
        temp = pasal.group(2)
    for pasal in res.keys():
        try:
            pasal_text = texts[res[pasal]['start']:res[pasal]['end']]
        except Exception:
            pasal_text = texts[res[pasal]['start']:]
        # print(pasal, texts)
        # print(pasal_text)
        filtered_ayat = re.finditer(r'(^Pasal[\n\s]+\d+[\n\s]+|\.[\n\s]+)(\(\d+\))', pasal_text)
        # print(re.sub('\n',' ',pasal_text))
        res[pasal]['sections'] = {}
        # print(pasal, pasal_text)
        for ayat in filtered_ayat:
            if len(res[pasal]['sections']) > 0:
                res[pasal]['sections'][temp]['end'] = ayat.start(2)
            res[pasal]['sections'][ayat.group(2)] = {'start': ayat.start(2)}
            temp = ayat.group(2)
            # print(ayat.group(2))
        # print(res)
    return res


def get_body_values(chapters, texts, part):
    next_part = {'chapters': 'parts', 'parts': 'paragraphs', 'paragraphs': 'articles', 'articles': 'sections'}
    # print(json.dumps(chapters, indent=4))
    for chapter in chapters.keys():
        try:
            part_text = texts[chapters[chapter]['start']:chapters[chapter]['end']]
        except Exception:
            part_text = texts[chapters[chapter]['start']:]
        if part != 'sections' and len(chapters[chapter][next_part[part]]) > 0:
            if part not in ('articles', 'sections'):
                first_next_key = list(chapters[chapter][next_part[part]].keys())[0]
                first_next_start = chapters[chapter][next_part[part]][first_next_key]['start']
                part_name = re.sub(chapter, '', part_text[:first_next_start])
                chapters[chapter]['name'] = part_name.strip()

            chapters[chapter][next_part[part]] = get_body_values(chapters[chapter][next_part[part]], part_text,
                                                                 next_part[part])
        elif part not in ('articles', 'sections'):
            print(list(chapters[chapter]['articles'].keys()))
            first_next_key = list(chapters[chapter]['articles'].keys())[0]
            first_next_start = chapters[chapter]['articles'][first_next_key]['start']
            part_name = re.sub(chapter, '', part_text[:first_next_start])
            chapters[chapter]['name'] = part_name.strip()
            chapters[chapter]['articles'] = get_body_values(chapters[chapter]['articles'], part_text, 'articles')
        else:
            values = re.sub(r'\s\+', ' ',
                            re.sub('^(Pasal[\s\n]+\d+|\(\d+\))', '', part_text)).strip()
            # print(part_text)
            # print(re.search(r'(?i)(Agar\s*)?setiap\s*orang\s*(yang\s*)?(dapat\s*)?mengetahui(nya)?[\w\W\n]+', values))
            # print(re.sub(r'\n', ' ', part_text.strip()))
            values = re.split(r'\n\d{1,3}\s?\.\s', values)
            details = []
            pos_tags = []
            refer_dict = []
            for idx in range(len(values)):
                values[idx], refers, temp, temp_postag = get_detail(values[idx].strip())
                details.append(temp)
                pos_tags.append(temp_postag)
                refer_dict.append(refers)
            chapters[chapter]['value'] = values
            chapters[chapter]['details'] = details
            chapters[chapter]['refers'] = refer_dict
            chapters[chapter]['pos_tag'] = pos_tags
    return chapters


def get_detail(texts):
    # print('\thhhh', texts, 'cccc')
    res = {}
    refer_to = re.finditer(r'(Pasal[\s\n]+\d+[\W\s\n]+)?(ayat[\s\n]+\(\d+\)[\W\s\n]+)?(huruf[\s\n]+[a-z][\W\s\n]+)?',
                           texts)
    refer_num = 0
    refer_dict = {}
    copy_texts = texts
    for refer in refer_to:
        if refer.group(0) != '':
            copy_texts = copy_texts.replace(re.sub(r'^[,.\s\n]+|[.,\s\n]+$', '', refer.group(0)), 'REFER' + str(refer_num))
            refer_dict['REFER' + str(refer_num)] = re.sub(r'[\W\s\n]+', ' ', refer.group(0)).strip()
            refer_num += 1
    texts = copy_texts
    refers = re.finditer(
        r'(Peraturan|Undang-Undang)[\s\W\n]+([A-Z][A-Za-z]+[\s\W\n]+)+Nomor[\W\s\n]+(\d+[\W\s\n]+Tahun[\W\s\n]+\d+|\d+(/[a-zA-Z0-9]+)+)[\W\s\n]+tentang[\W\s\n]+',
        texts)
    start = None
    for refer in refers:
        if refer.group(0) != '':
            if start is None:
                start = refer.start()
                continue
            temp = texts[start: refer.start(0)]
            refer_text = re.search(
                r'(^(Peraturan|Undang-Undang)[\s\W\n]+([A-Z][A-Za-z]+[\s\W\n]+)+Nomor[\W\s\n]+(\d+[\W\s\n]+Tahun[\W\s\n]+\d+|\d+(/[a-zA-Z0-9]+)+)[\W\s\n]+tentang[\W\s\n]+(([A-Z][a-zA-Z0-9]+|\d+|dan)[\W\s\n]+)*([A-Z][a-zA-Z0-9]+|\d+))[\W\s\n]+',
                temp)
            # print('>>>', refer_text.group(1))
            copy_texts = copy_texts.replace(refer_text.group(1), 'REFER' + str(refer_num))
            refer_dict['REFER' + str(refer_num)] = re.sub('[\s\n]+', ' ', refer_text.group(1))
            refer_num += 1
            start = refer.start(0)
    texts = copy_texts
    # print(copy_texts)
    # print(json.dumps(refer_dict, indent=4))
    details = re.finditer(r'[:;,.]([\s\n]*(dan|atau|dan\s*/\s*atau))?[\s\n]+([a-z]{1,2})\.', texts)
    temp = None
    for detail in details:
        if len(res) > 0:
            res[temp]['end'] = detail.start(3)
        res['huruf ' + detail.group(3)] = {'start': detail.start(3)}
        temp = 'huruf ' + detail.group(3)
    detail_text = {}
    for res_k in res.keys():
        try:
            detail_text[res_k] = re.sub('(dan\s*/\s*atau|dan|atau)[\s\n]+$', '',
                                        texts[res[res_k]['start']:res[res_k]['end']])
        except Exception:
            detail_text[res_k] = re.sub(r',[\s]+\n[\W\w\n]+|\.$', '', texts[res[res_k]['start']:])
    pos_tags = {'detail': {}}
    for res_k in res.keys():
        texts = texts.replace(detail_text[res_k], res_k + ', ')
        texts = re.sub(r',[\s\n]+,', ', ', re.sub(':', '', re.sub(r',[\s\n]+\.[\s\n]*$', '.', texts)))
        detail_text[res_k] = re.sub(r'[\s\n]+', ' ',
                                    re.sub(r'^[a-z]\.|[;,.][\s\n]+$', '', detail_text[res_k])).strip() + '.'
        # print(detail_text[res_k])
        pos_tags['detail'][res_k] = pos_tag(detail_text[res_k])
    texts = re.sub(r'[\s\n]+', ' ', texts)
    # print('ttt',texts)
    pos_tags['texts'] = pos_tag(texts)
    return texts, refer_dict, detail_text, pos_tags


def pos_tag(texts):
    res = {}
    # print('kkk', texts.strip())
    doc = nlp(texts)
    res['nodes'] = []
    res['edges'] = []
    for idx_sentence in range(len(doc.sentences)):
        for word in doc.sentences[idx_sentence]._words:
            node = {'sentence_id': idx_sentence}
            node['id'] = word._id
            node['text'] = word._text
            node['type'] = word._upos.lower()
            node['min'] = word._id
            node['max'] = word._id
            res['nodes'].append(node)
        for key in range(len(doc.sentences[idx_sentence]._dependencies)):
            relation = (doc.sentences[idx_sentence]._dependencies)[key]
            edge = {}
            word_source = relation[0]
            word_target = relation[2]
            edge['sentence_id'] = idx_sentence
            edge['id'] = key
            edge['source'] = word_source._id
            edge['target'] = word_target._id
            edge['rel_name'] = relation[1]
            edge['state'] = 1
            res['edges'].append(edge)
    # print(res['nodes'])
    # print(res['edges'])
    return res


def get_closing_part(texts):
    month = 'januari|februari|maret|april|mei|juni|juli|agustus|september|oktober|november|desember'
    res = {}
    # print(texts)
    filtered_text = re.search(r'(Agar\s*)?[sS]e(tiap|mua)\s*[oO]rang\s*(yang\s*)?(dapat\s*)?mengetahui(nya)?[\w\W\n]+',
                              texts).group(0).strip()

    # print(filtered_text)
    res['promulgation'] = {
        'place': re.search('berita[\s\n]*negara|lembaran[\s\n]*negara|lembaran[\s\n]*daerah|berita[\s\n]*daerah',
                           filtered_text.lower()).group(0)}
    res['enactment'] = {}
    # print(filtered_text)
    try:
        parts = {'enactment': re.search(r'Ditetapkan([\w\W\n]+)Diundangkan', filtered_text).group(1).strip(),
                 'promulgation': re.search(r'Diundangkan([\w\W\n]+)', filtered_text).group(1).strip()}
    except Exception:
        return res
    for key in parts.keys():
        locations = [i for i in re.finditer(r'di([\w\W\n]+)pada[\s\n]+((tanggal)?)', parts[key])]
        if len(locations) == 1:
            res[key]['location'] = re.sub(r'[\n\s]+', ' ',
                                          locations[0].group(1)).strip()
        elif len(locations) == 2:
            res['enactment']['location'] = re.sub(r'[\n\s]+', ' ', locations[0].group(1)).strip()
            res['promulgation']['location'] = re.sub(r'[\n\s]+', ' ', locations[1].group(1)).strip()
        dates = [i for i in re.finditer(r'tanggal[\s\n]+(\d+[\s\n]+(' + month + r')[\s\n]+\d+)', parts[key].lower())]
        if len(dates) == 1:
            res[key]['date'] = re.sub(r'[\n\s]+', ' ', dates[0].group(1)).strip()
        elif len(dates) == 2:
            res['enactment']['date'] = re.sub(r'[\n\s]+', ' ', dates[0].group(1)).strip()
            res['promulgation']['date'] = re.sub(r'[\n\s]+', ' ', dates[1].group(1)).strip()
        officials = [i for i in re.finditer(r'([A-Z\s]+),', parts[key])]
        if len(officials) == 1:
            res[key]['official'] = re.sub(r'[\n\s]+', ' ', officials[0].group(1)).strip()
        elif len(officials) == 2:
            res['enactment']['official'] = re.sub(r'[\n\s]+', ' ', officials[0].group(1)).strip()
            res['promulgation']['official'] = re.sub(r'[\n\s]+', ' ', officials[1].group(1)).strip()
        official_names = [i for i in re.finditer(r'(?i)ttd[\s\n]+([A-Z\s]+)', parts[key])]
        if len(official_names) == 1:
            res[key]['official_name'] = re.sub(r'[\n\s]+', ' ', official_names[0].group(1)).strip()
        elif len(official_names) == 2:
            res['enactment']['official_name'] = re.sub(r'[\n\s]+', ' ', official_names[0].group(1)).strip()
            res['promulgation']['official_name'] = re.sub(r'[\n\s]+', ' ', official_names[1].group(1)).strip()
    return res


nlp = stanza.Pipeline(lang='id', use_gpu=True)
file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')
last_error_file = open('last_error.txt', 'r+')
last_error = last_error_file.readline()
last_error_file.close()
file_num = 0
state = True
next = True
# res = None
failed = []

while state:
    text = file_list.readline()
    if text == '':
        state = False
    print(file_num, text)
    text = text.strip()
    # if text != 'http://peraturan.go.id/common/dokumen/bn/2013/bn684-2013.pdf':
    #     continue
    filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/' + (
        re.sub('_pdf', '.txt', re.sub('(%20)|([\\._]+)', '_',
                                      re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '', text)))).lower()
    # if (filesource != last_error and next) or file_num < 31391:
    #     file_num += 1
    #     continue
    # else:
    #     next = False
    # if filesource == 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/ln/1950/uu7-1950.txt':
    #     continue
    filetarget = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '', re.sub('(%20)|([\\._]+)', '_',
                                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                 '', text)))).group(1).lower()) + '.json'
    dirName = re.sub('/[^/]+$', '', filetarget)
    try:
        # Create target Directory
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except Exception:
        None
    try:
        # filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/perda/2019/perwal_no_5_tahun_2019.txt'
        file = open(filesource, encoding="utf8")
    except Exception:
        continue
    if re.search('putusan', filesource):
        continue
    text = file.read()
    text = re.sub('\n+\s*\d+\W*([Nn][Oo]\\.\s*(-?\s*\d+\s*-?\s*)+)?\n+', '\n',
                  re.sub(r'\n+(\s*\d+\s*\n+)?', '\n',
                         re.sub('\\nwww.djpp.(depkumham|kemenkumham).go.id|www.peraturan.go.id\\n', '\n',
                                re.sub(r'[^a-zA-Z\d\n./:(\-,);]', ' ',
                                       re.sub(r'\n+', '\n',
                                              re.sub('\\n\\s+\\n', '\\n\\n', text)))))).strip()
    # print(text)
    if re.match(r'^[\s\n\W]*$', text):
        continue
    if re.search('(?i)/[^/]*tln|lmp[^/]*$', filesource):
        continue
    try:
        result = {}
        result['title'] = get_title(text)
        # print(json.dumps(result['title'], indent=4))
        result['considerans'] = get_considerans(text)
        # print(json.dumps(result['considerans'], indent=4))
        result['law_based'] = get_law_based(text)
        # print(json.dumps(result['law_based'], indent=4))
        result['dictum'] = get_dictum(text)
        # print(json.dumps( result['dictum'], indent=4))
        result['closing_part'] = get_closing_part(text)
        # print(json.dumps(closing_part, indent=4))
        result['body'] = get_body_part(text)
        with open(filetarget, 'w') as json_file:
            json.dump(result, json_file, indent=4)
        # print(json.dumps(result, indent=4))
        #     # print(res)
        #     # res.to_csv('D:/Ninggar/Mgstr/Semester 2/Data/files/parsed_files/perda/2017/perwal_no_02_tahun_2017_petunjuk_teknis_pembiayaan_jaminan_kesehatan_masyarakat_miskin_di_luar_kuota_penerima_bantuan_iuran_jaminan_kesehatan_dan_bantuan_sosial_tidak_terencana_bagi_orang_terlantar.csv', index=False)
        #     res.to_csv(filetarget, index=False)
        file_num += 1
    #     # print(file_num, filesource)
    #     # break
    except RuntimeError:
        print('failed', filesource)
        with open('last_error.txt', 'w') as text_file:
            text_file.write(filesource)
        print(Exception.with_traceback())
    except Exception:
        failed.append(filesource)
        print('failed', filesource)
        # with open('last_error.txt', 'w') as text_file:
        #     text_file.write(filesource)
        # print(Exception.with_traceback())
        # state = False
        # print(Exception.with_traceback())
    # print(filesource)
    break
print(failed)
print(len(failed))
print(file_num)
file_list.close()
