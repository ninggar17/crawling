import json
import os
import re
import ast
import pandas
import stanfordnlp
import warnings
# import pyspark
from pyspark import SparkFiles, SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *

warnings.simplefilter("ignore")

# path = "test.txt"
# conf = SparkConf().setMaster('local') \
#     .setAppName('legalNLP') \
#     .set('spark.driver.memory', '2g') \
#     .set('spark.executor.instances', '4') \
#     .set('spark.executor.memory', '2g') \
#     .set('spark.executor.cores', '4')
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)
# sqlContext = SQLContext(sc)
file_list = open('C:/Users/Bukalapak/PycharmProjects/crawling/crawl/pdf_list', 'r+')
file_num = 0
state = True
res = None
line = 0
failed = []

# stanfordnlp.download('id')  # This downloads the Indonesian models for the neural pipeline
nlp = stanfordnlp.Pipeline(lang='id', use_gpu=False)  # This sets up a default neural pipeline in Indonesian
# relationships = []


# def udf_wrapper(return_type):
#     def udf_func(func):
#         return udf(func, returnType=return_type)
#
#     return udf_func


# @udf_wrapper(StringType())
# def ner(sentence):
#     doc = nlp(sentence)
#     relationships = []
#     for idx_sentence in range(len(doc.sentences)):
#         for relation in doc.sentences[idx_sentence]._dependencies:
#             relationship = {}
#             word_source = relation[0]
#             word_target = relation[2]
#             relationship['sentence_id'] = idx_sentence
#             relationship['source'] = {'text': word_source._text, 'word_type': word_source._upos,
#                                       'word_feats': word_source._feats}
#             relationship['target'] = {'text': word_target._text, 'word_type': word_target._upos,
#                                       'word_feats': word_target._feats}
#             relationship['rel_name'] = relation[1]
#             relationships.append(relationship)
#     return json.dumps(relationships)


while state:
    text = file_list.readline()
    # text = 'http://peraturan.go.id/common/dokumen/bn/2009/bn116-2009.pdf'
    print(line, text)
    line += 1
    if line < 27348:
        continue
    # print('start')
    if text == '':
        state = False
    text = text.strip()
    filesource = 'D:/Ninggar/Mgstr/Semester 2/Data/files/fix_position_v1/' + (
        re.search(r'(.{,100})', re.sub('(%20)|([\\._]+)', '_', re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                      '', text)))).group(1).lower() + '.csv'
    filetarget = 'D:/Ninggar/Mgstr/Semester 2/Data/files/postagging/' + (
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
        # data = data.astype(str)
        # data_spark = spark.createDataFrame(data)
        # data = pandas.read_csv('D:/Ninggar/Mgstr/Semester 2/Data/files/ayat_parsed_files/perda/2018/2018-33.csv')
    except FileNotFoundError:
        print('not found')
        continue
    except Exception:
        failed.append(filesource)
        continue
    # print(data)
    # print(data.dtypes)
    relationships = []
    # data_spark = data_spark.withColumn('nlp_res', ner(data_spark['value']))
    # res = data_spark.toPandas()
    # print(res)
    for row in data.iterrows():
        # print(re.sub(r'\n', ' ', row[1]['value']))
        try:
            values = ast.literal_eval(str(row[1]['value']))
        except Exception:
            values = [str(row[1]['value'])]
        # print(values)
        # print(json.dumps(values, indent=4))
        relationship = {
            'partition': row[1]['partition'],
            'part_1': row[1]['part_1'],
            'part_2': row[1]['part_2'],
            'start_pos': row[1]['start_pos'],
            'ayat': row[1]['ayat'],
            'filename': row[1]['filename'],
            'value': row[1]['value']
        }
        try:
            for value in values:
                # print(text)
                # print(re.sub(r'\n', ' ', value))
                text = str(re.sub(r'\n', ' ', value))
                # print(text)
                doc = nlp(text)
                res_nlp = []
                for idx_sentence in range(len(doc.sentences)):
                    for relation in doc.sentences[idx_sentence]._dependencies:
                        res_sentence = []
                        word_source = relation[0]
                        word_target = relation[2]
                        res_sentence.append({
                            'source': {'text': word_source._text,
                                       'word_type': word_source._upos,
                                       'word_feats': word_source._feats},
                            'target': {'text': word_target._text,
                                       'word_type': word_target._upos,
                                       'word_feats': word_target._feats},
                            'rel_name': relation[1]
                        })
                        res_nlp.append(res_sentence)
                relationship['res_nlp'] = json.dumps(res_nlp)
                relationships.append(relationship)
                # doc = nlp(text)
                # for idx_sentence in range(len(doc.sentences)):
                #     for relation in doc.sentences[idx_sentence]._dependencies:
                #         relationship = {}
                #         word_source = relation[0]
                #         word_target = relation[2]
                #         relationship['partition'] = row[1]['partition']
                #         relationship['part_1'] = row[1]['part_1']
                #         relationship['part_2'] = row[1]['part_2']
                #         relationship['start_pos'] = row[1]['start_pos']
                #         relationship['ayat'] = row[1]['ayat']
                #         relationship['filename'] = row[1]['filename']
                #         relationship['value'] = row[1]['value']
                #         relationship['sentence_id'] = idx_sentence
                #         relationship['source'] = {'text': word_source._text, 'word_type': word_source._upos,
                #                                   'word_feats': word_source._feats}
                #         relationship['target'] = {'text': word_target._text, 'word_type': word_target._upos,
                #                                   'word_feats': word_target._feats}
                #         relationship['rel_name'] = relation[1]
                #         relationships.append(relationship)
        except Exception:
            relationship = {'partition': row[1]['partition'], 'part_1': row[1]['part_1'],
                            'part_2': row[1]['part_2'], 'start_pos': row[1]['start_pos'], 'ayat': row[1]['ayat'],
                            'filename': row[1]['filename'], 'value': row[1]['value'], 'sentence_id': None,
                            'source': None, 'target': None, 'rel_name': None}
            relationships.append(relationship)
            pass
    data = pandas.DataFrame(relationships)
    data.to_csv(filetarget)
    # print(filetarget)
    # break
file_list.close()
print(json.dumps(failed, indent=4))
