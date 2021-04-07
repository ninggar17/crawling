import json
import re
from datetime import datetime

from pandasql import sqldf
from rdflib import URIRef, BNode, Literal, Graph
from rdflib.namespace import RDF, RDFS, FOAF, OWL, NamespaceManager, Namespace
import pandas

g = Graph()
detail_peraturan_df = pandas.read_csv('C:/Users/Bukalapak/PycharmProjects/crawling/crawl/legal_datail.csv',
                                      index_col='Unnamed: 0')
general_peraturan_df = pandas.read_csv('C:/Users/Bukalapak/PycharmProjects/crawling/crawl/data_peraturan.csv')

detail_peraturan_df = sqldf("""
    select
        detail_peraturan_df.*,
        general_peraturan_df.peraturan nama
    from
        detail_peraturan_df
    join
        general_peraturan_df
    on
        detail_peraturan_df.url=general_peraturan_df.url_peraturan
""")

prefix_object = Namespace('https://www.example.org/')
prefix_relation = Namespace('https://www.example.org/')
i = 0
manager = NamespaceManager(g)
manager.bind('legal', prefix_object)
manager.bind('owl', OWL)


def detail_rel(node, detail):
    """

    :param node: url dari peraturan
    :param detail: rincian dari peraturan
    """
    for key in detail.keys():
        rel = prefix_relation[re.sub(r'\W+', '_', key.lower())]
        if detail[key] != '':
            if re.search(r'tahun', key.lower()):
                obj = Literal(int(detail[key]))
            elif re.search(r'tgl', key.lower()):
                try:
                    obj = Literal(datetime.strptime(detail[key], "%Y-%m-%d").date())
                except Exception:
                    continue
            else:
                obj = Literal(detail[key].lower())
            g.add((node, rel, obj))


def category_rel(node, categories):
    rel = prefix_relation.has_category
    category_map = {
        'bmn': 'Barang Milik Negara',
        'pnbp': 'Penerimaan Negara Bukan Pajak',
        'bumn': 'Badan Usaha Milik Negara',
        'ham': 'Hak Asasi Manusia',
        'apbn': 'Anggaran Pendapatan dan Belanja Negara',
        'bmkg': 'Badan Meteorologi, Klimatologi, dan Geofisika'
    }
    for category in categories:
        category_node = prefix_object[re.sub(r'[\s.&,]+', '_', category)]
        g.add((node, rel, category_node))
        g.add((category_node, RDF.type, prefix_object.Legal_Category))
        g.add((category_node, FOAF.name, Literal(category)))
        try:
            g.add(g.add((category_node, FOAF.name, Literal(category_map[category.lower()]))))
        except Exception:
            pass
    g.add((prefix_object.Legal_Category, RDF.type, OWL.Class))


def peraturan_rel(node, relations):
    for relation in relations.keys():
        rel = prefix_relation[re.sub(r'\W+', '_', relation.lower())]
        for obj_peraturan in relations[relation]:
            id_obj = re.search(r'\w+$', obj_peraturan).group(0)
            g.add((node, rel, prefix_object['P' + id_obj]))


for row in detail_peraturan_df.iterrows():
    i += 1
    print(i, row[1]['url'])
    id_peraturan = re.search(r'\w+$', row[1]['url']).group(0)
    peraturan = prefix_object['P' + id_peraturan]
    nama_peraturan = Literal(row[1]['nama'].lower())
    detail_peraturan = re.sub(r"'\s*:\s*'", '" : "', row[1]['detail'])
    detail_peraturan = re.sub(r"'\s*,\s*'", '" , "', detail_peraturan)
    detail_peraturan = re.sub(r"^\s*{\s*'", '{"', detail_peraturan)
    detail_peraturan = re.sub(r"'\s*}\s*$", '"}', detail_peraturan)
    detail_peraturan = json.loads(detail_peraturan)

    tipe_peraturan = prefix_object[re.sub(r'\W+', '_', detail_peraturan['Jenis Peraturan'])]
    g.add((tipe_peraturan, FOAF.name, Literal(detail_peraturan['Jenis Peraturan'])))
    g.add((peraturan, RDF.type, tipe_peraturan))
    g.add((peraturan, FOAF.name, nama_peraturan))
    g.add((peraturan, prefix_relation.source, Literal(row[1]['url'])))
    g.add((tipe_peraturan, RDF.type, OWL.Class))
    detail_peraturan.pop('Jenis Peraturan')
    detail_rel(peraturan, detail_peraturan)
    list_categories = json.loads(row[1]['categories'].replace("'", '"'))
    category_rel(peraturan, list_categories)
    list_relation = json.loads(row[1]['relations'].replace("'", '"'))
    superclass_type = json.loads(row[1]['files'].replace("'", '"'))
    superclass_map = {'ln': 'Lembaran Negara',
                      'bn': 'Berita Negara',
                      'perda': 'Peraturan Daerah',
                      'putusan': 'Putusan',
                      'lain-lain': 'Peraturan Lain-Lain'}
    if len(superclass_type) > 1:
        superclass_type = re.sub('http://peraturan.go.id/common/dokumen/', '', superclass_type[0])
        superclass_type = re.sub('/.*', '', superclass_type)
        supeclass_node = prefix_object[re.sub(r'\W', '_', superclass_map[superclass_type])]
        g.add((supeclass_node, RDF.type, OWL.Class))
        g.add((supeclass_node, FOAF.name, Literal(superclass_type.lower())))
        if superclass_type != 'perda':
            g.add((tipe_peraturan, RDFS.subClassOf, supeclass_node))
    peraturan_rel(peraturan, list_relation)
    # if i > 10:
    #     break

# ouput = open('legal_triple_v1.ttl', 'w')
g.serialize(destination='legal_triple_v1.ttl', format='turtle')
