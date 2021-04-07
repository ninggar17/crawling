import json
import os
import re
import time

import pandas
from pandasql import sqldf

pandas.set_option('display.max_rows', 120)
pandas.set_option('display.max_colwidth', None)
pandas.set_option('display.max_columns', None)


def get_body_values(chapters, part):
    next_part = {'chapters': 'parts', 'parts': 'paragraphs', 'paragraphs': 'articles', 'articles': 'sections'}
    stop = False
    for chapter in chapters.keys():
        print(chapter)
        if stop:
            return stop
        if part != 'sections' and len(chapters[chapter][next_part[part]]) > 0:
            stop = get_body_values(chapters[chapter][next_part[part]], next_part[part])
        elif part not in ('articles', 'sections'):
            stop = get_body_values(chapters[chapter]['articles'], 'articles')
        else:
            for postag in chapters[chapter]['pos_tag']:
                if len(postag['detail']) > 0:
                    for d_key in postag['detail'].keys():
                        stop = postag_handle(postag['detail'][d_key])

                stop = stop or postag_handle(postag['texts'])
                if stop:
                    return stop


def restructure(graph, node, n_text, new_graph, sentence_id, del_items):
    nodes = graph['V']
    edges = graph['E']
    type_source = nodes[(nodes['id'] == node) & (nodes['sentence_id'] == sentence_id)]['type'].array[0]
    source_name = nodes[(nodes['id'] == node) & (nodes['sentence_id'] == sentence_id)]['text'].array[0]
    copy_node = node
    min_source = nodes[(nodes['id'] == node) & (nodes['sentence_id'] == sentence_id)]['min'].array[0]
    max_source = nodes[(nodes['id'] == node) & (nodes['sentence_id'] == sentence_id)]['max'].array[0]
    min_text = min_source
    max_text = max_source
    type_text = type_source
    e_outs = sqldf('''
        select
            source,
            target,
            abs(source - target),
            rel_name,
            id
        from
            edges
        where
                source = {node}
            and
                state = 1
            and
                sentence_id = {sentence_id}
        order by 3
    '''.format(node=node, sentence_id=str(sentence_id)))  # .target
    for e_out in e_outs.iterrows():
        neighbor = e_out[1]['target']
        type_target = nodes[(nodes['id'] == neighbor) & (nodes['sentence_id'] == sentence_id)]['type'].array[0]
        target_name = nodes[(nodes['id'] == neighbor) & (nodes['sentence_id'] == sentence_id)]['text'].array[0]
        min_target = nodes[(nodes['id'] == neighbor) & (nodes['sentence_id'] == sentence_id)]['min'].array[0]
        max_target = nodes[(nodes['id'] == neighbor) & (nodes['sentence_id'] == sentence_id)]['max'].array[0]
        e_id = e_out[1]['id']
        rel_type = e_out[1]['rel_name']
        # edges[(edges.source == copy_node) & (edges.target == neighbor) & (edges.sentence_id == sentence_id)][
        #     'rel_name'].array[0]
        if rel_type == 'punct':
            new_graph['E'].loc[(new_graph['E'].source == copy_node) & (new_graph['E'].target == neighbor) & (
                    new_graph['E'].sentence_id == sentence_id), "state"] = 0
            continue
        if len(edges[(edges['source'] == neighbor) & (edges['sentence_id'] == sentence_id)]['target']) > 0:
            res, res_id, min_res, max_res, type_res, del_items = restructure(graph, neighbor, target_name, new_graph,
                                                                             sentence_id, del_items)
        else:
            res = target_name
            res_id = neighbor
            min_res = min_target
            max_res = max_target
            type_res = type_target
        # print(rel_type, type_res, type_text, n_text, ',', res)
        # print(len(edges[(edges['source'] == 20) & (edges['target'] == 23) & (edges['state'] == 1)]))
        if (rel_type in ('compound', 'flat', 'mark', 'det') and not (type_res == 'num' or type_text == 'num') and (
                min_text - max_res == 1 or min_res - max_text == 1)) or (
                rel_type == 'nummod' and type_res == 'num' and type_text == 'num' and (
                min_text - max_res < 3 or min_res - max_text < 3)):
            # print('jjj')
            if min_res - max_text > 0:
                target_name = n_text + ' ' + res
            else:
                target_name = res + ' ' + n_text
            min_target = min(min_res, min_text)
            max_target = max(max_res, max_text)
            # sentence_id = nodes[nodes['id'] == neighbor]['sentence_id'].array[0]
            neighbor = len(new_graph['V'][new_graph['V']['sentence_id'] == sentence_id])
            if type_res == 'verb' or type_text == 'verb':
                type_target = 'verb'
            elif type_res == type_text:
                type_target = type_text
            else:
                type_target = 'phrase'
            new_v = {'sentence_id': sentence_id, 'id': neighbor, 'text': target_name, 'type': type_target,
                     'min': min_target, 'max': max_target}
            new_graph['V'] = new_graph['V'].append(new_v, ignore_index=True)
            new_graph['E'].loc[((new_graph['E'].source == node) & (new_graph['E'].target != res_id) &
                                (new_graph['E'].sentence_id == sentence_id)) | (
                                       (new_graph['E'].source == res_id) & (new_graph['E'].target != node) &
                                       (new_graph['E'].sentence_id == sentence_id)), "source"] = neighbor
            new_graph['E'].loc[((new_graph['E'].target == node) & (new_graph['E'].source != res_id) &
                                (new_graph['E'].sentence_id == sentence_id)) | (
                                       (new_graph['E'].target == res_id) & (new_graph['E'].source != node) &
                                       (new_graph['E'].sentence_id == sentence_id)), "target"] = neighbor
            new_e1 = {'sentence_id': sentence_id,
                      'id': len(new_graph['E'][new_graph['E']['sentence_id'] == sentence_id]), 'source': neighbor,
                      'target': node,
                      'rel_name': rel_type, 'state': 1}
            new_graph['E'] = new_graph['E'].append(new_e1, ignore_index=True)
            new_e2 = {'sentence_id': sentence_id,
                      'id': len(new_graph['E'][new_graph['E']['sentence_id'] == sentence_id]), 'source': neighbor,
                      'target': res_id,
                      'rel_name': rel_type, 'state': 1}
            new_graph['E'] = new_graph['E'].append(new_e2, ignore_index=True)
            new_graph['E'].loc[(new_graph['E'].source == node) & (new_graph['E'].target == res_id) &
                               (new_graph['E'].sentence_id == sentence_id), "state"] = 0
            n_text = target_name
            node = neighbor
            min_text = min_target
            max_text = max_target
            type_text = type_target
            del_items.append(e_id)
    return n_text, node, min_text, max_text, type_text, del_items


def special_merge(graph, words, word_type):
    nodes = graph['V']
    edges = graph['E']
    merged_nodes = nodes[nodes['text'].isin(words)].sort_values('id')
    ids = []
    new_text = ''
    order = 0
    sentence_id = None
    for node in merged_nodes.iterrows():
        # print('order', order, words[order], node[1]['text'])
        # print(ids, node[1]['id'])
        if len(ids) > 0 and ids[order - 1] + 1 == node[1]['id'] and node[1]['text'] == words[order] and sentence_id == \
                node[1]['sentence_id']:
            # print('merge', new_text, node[1]['text'])
            ids.append(node[1]['id'])
            new_text += ' ' + node[1]['text']
            order += 1
        elif node[1]['text'] == words[0]:
            ids.append(node[1]['id'])
            new_text = node[1]['text']
            order = 1
            sentence_id = node[1]['sentence_id']
        if order == len(words):
            # print(ids)
            # print(new_text)
            new_id = len(nodes)
            nodes = nodes.append({'sentence_id': sentence_id, 'id': new_id, 'text': new_text, 'type': word_type,
                                  'min': min(ids), 'max': max(ids)}, ignore_index=True)
            for idx in ids:
                #     is any compound?
                try:
                    id_edge = edges[(edges['source'] == idx) & (edges['rel_name'] == 'compound') &
                                    (edges['sentence_id'] == sentence_id)]['id'].array[0]
                    id_target = edges[(edges['source'] == idx) & (edges['rel_name'] == 'compound') &
                                      (edges['sentence_id'] == sentence_id)]['target'].array[0]
                    # print(id_edge, id_target)
                    edges.loc[(edges['target'] == idx) & (edges['sentence_id'] == sentence_id), "target"] = id_target
                    edges.loc[(edges['source'] == idx) & (edges['sentence_id'] == sentence_id) & (
                        ~edges['rel_name'].isin(['cc'])), "source"] = id_target
                    edges.loc[(edges['source'] == idx) & (edges['sentence_id'] == sentence_id) & (
                        edges['rel_name'].isin(['cc'])), "source"] = new_id
                    edges.loc[(edges['id'] == id_edge) & (edges['sentence_id'] == sentence_id), "state"] = 0
                except Exception:
                    # pass
                    edges.loc[(edges['target'] == idx) & (edges['sentence_id'] == sentence_id), "target"] = new_id
                    edges.loc[(edges['source'] == idx) & (edges['sentence_id'] == sentence_id), "source"] = new_id
                if new_text == 'sesuai dengan':
                    non_case = edges[(edges['target'] == new_id) & (edges['sentence_id'] == sentence_id) & (
                        ~edges['rel_name'].isin(['case']))]['source'].array[0]
                    edges.loc[(edges['source'] == new_id) & (edges['sentence_id'] == sentence_id) & (
                        ~edges['rel_name'].isin(['case'])), 'source'] = non_case
                    edges.loc[(edges['target'] == new_id) & (edges['sentence_id'] == sentence_id) & (
                        ~edges['rel_name'].isin(['case'])), 'state'] = 0
            edges.loc[(edges['source'] == edges['target']) & (edges['sentence_id'] == sentence_id), "state"] = 0
            order = 0
    graph['V'] = nodes
    graph['E'] = edges
    return False, graph


def postag_handle(postag, stop=False):
    special_phrase = {
        'atas nama': [['atas', 'nama'], 'adp'],
        'sebagaimana dimaksud': [['sebagaimana', 'dimaksud'], 'phrase'],
        # 'dan/atau': [['dan/atau'], 'cconj'],
        'dan / atau': [['dan', '/', 'atau'], 'cconj'],
        'Dalam hal': [['Dalam', 'hal'], 'case'],
        'dalam hal': [['dalam', 'hal'], 'case'],
        'sesuai dengan': [['sesuai', 'dengan'], 'adp'],
        'sampai dengan': [['sampai', 'dengan'], 'adp'],
        'paling sedikit': [['paling', 'sedikit'], 'case'],
        'paling banyak': [['paling', 'banyak'], 'case'],
        'paling paling': [['paling', 'lama'], 'case'],
    }
    nodes = pandas.DataFrame(postag['nodes'])
    edges = pandas.DataFrame(postag['edges'])
    G = {'V': nodes, 'E': edges}
    for sentence_id in range(max(G['V']['sentence_id']) + 1):
        new_v = {'sentence_id': sentence_id, 'id': 0, 'text': 'root', 'type': 'none',
                 'min': 0, 'max': 0}
        G['V'] = G['V'].append(new_v, ignore_index=True)
    # print(G['V'])
    # print(G['E'])
    for phrase in special_phrase.values():
        if len(G['V'][G['V']['text'].isin(phrase[0])]) > 1:
            stop, G = special_merge(G, phrase[0], phrase[1])
    copy_node = G['V'].copy()
    copy_edge = G['E'].copy()
    new_G = {'V': copy_node, 'E': copy_edge}
    # print(G['V'])
    # print(G['E'])

    for sentence_id in range(max(G['V']['sentence_id']) + 1):
        dum1, dum2, dum3, dum4, dum5, del_items = restructure(G, 0, text, new_G, sentence_id, [])
        new_G['E'].loc[new_G['E']['id'].isin(del_items), 'state'] = 0
    nodes = new_G['V']
    edges = new_G['E']
    del_e = sqldf('''
            select
                edges.id,
                sources.text,
                targets.text
            from
                edges
            join
                nodes as sources
            on
                sources.id=edges.source
            join
                nodes as targets
            on
                targets.id=edges.target
            where
                rel_name in ('compound', 'flat', 'mark', 'det')
            and
                sources.text like '%'||targets.text||'%'
            and
                state = 1
            ''')
    # print(del_e)
    new_G['E'].loc[new_G['E'].id.isin(del_e.id.unique()), 'state'] = 0
    new_G['E'].loc[new_G['E'].rel_name == 'punct', 'state'] = 0
    # nodes = new_G['V']
    # edges = new_G['E']
    # del_e = sqldf('''
    #     select
    #         edges.id
    #         --sources.text,
    #         --targets.text,
    #         --rel_name
    #     from
    #         edges
    #     join
    #         nodes as sources
    #     on
    #         sources.id = edges.source
    #     join
    #         nodes as targets
    #     on
    #         targets.id = edges.target
    #     where
    #         rel_name in ('det', 'flat', 'mark', 'amod', 'punct', 'compound', 'advmod')
    #     and
    #         state = 1
    # ''')
    # new_G['E'].loc[new_G['E']['id'].isin(del_e.id.unique()), "state"] = 0
    conjs = new_G['E'][(new_G['E']['rel_name'] == 'cc') & (new_G['E']['state'] == 1)][
        ['id', 'sentence_id', 'source', 'target']]
    # print(new_G['V'])
    # print(new_G['E'][new_G['E']['state'] == 1])
    for conj in conjs.iterrows():
        # print(conj)
        v_conj = conj[1]['source']
        # min_vconj = new_G['V'][new_G['V']['id'] == v_conj]['min'].array[0]
        # max_vconj = new_G['V'][new_G['V']['id'] == v_conj]['max'].array[0]
        name_vconj = new_G['V'][new_G['V']['id'] == v_conj]['text'].array[0]
        cc = conj[1]['target']
        # min_cc = new_G['V'][new_G['V']['id'] == cc]['min'].array[0]
        max_cc = new_G['V'][new_G['V']['id'] == cc]['max'].array[0]
        sentence_id = conj[1]['sentence_id']
        # print(v_conj, name_vconj)
        try:
            root_conj = new_G['E'][
                (new_G['E']['target'] == v_conj) & (new_G['E']['rel_name'] == 'conj') &
                (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1)][
                'source'].array[0]
        except Exception:
            new_G['E'].loc[(new_G['E']['source'] == v_conj) & (new_G['E']['rel_name'] == 'conj') &
                           (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1), 'state'] = 0
            new_G['E'].loc[(new_G['E']['source'] == v_conj) & (new_G['E']['rel_name'] == 'cc') &
                           (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1), 'state'] = 0
            continue
        min_rconj = \
            new_G['V'][(new_G['V']['id'] == root_conj) & (new_G['V']['sentence_id'] == sentence_id)]['min'].array[0]
        # max_rconj = new_G['V'][new_G['V']['id'] == root_conj]['max'].array[0]
        child_conj = new_G['E'][
            (new_G['E']['source'] == root_conj) & (new_G['E']['rel_name'] == 'conj') & (new_G['E']['state'] == 1)][
            'target'].array
        #     child_conj.append(root_conj)
        #     print(root_conj, child_conj)
        new_G['E'].loc[(new_G['E'].id == conj[1]['id']) & (new_G['E']['sentence_id'] == sentence_id), "state"] = 0
        new_G['E'].loc[(new_G['E']['source'] == root_conj) & (new_G['E']['rel_name'] == 'conj') &
                       (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1), "state"] = 0
        for child in child_conj:
            min_child = \
                new_G['V'][(new_G['V']['id'] == child) & (new_G['V']['sentence_id'] == sentence_id)]['min'].array[0]
            # max_child = new_G['V'][new_G['V']['id'] == child]['max'].array[0]
            new_e = {'sentence_id': sentence_id, 'id': len(new_G['E']), 'source': cc, 'target': child,
                     'rel_name': 'conj', 'state': 1}
            child_edges = new_G['E'][
                (new_G['E']['source'] == child) & (new_G['E']['rel_name'] != 'conj') &
                (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1)][
                ['id', 'rel_name', 'target']]
            for c_e in child_edges.iterrows():
                e_id = c_e[1]['id']
                e_target = c_e[1]['target']
                min_e = \
                    new_G['V'][(new_G['V']['id'] == e_target) & (new_G['V']['sentence_id'] == sentence_id)][
                        'min'].array[0]
                max_e = \
                    new_G['V'][(new_G['V']['id'] == e_target) & (new_G['V']['sentence_id'] == sentence_id)][
                        'max'].array[0]
                # print(e_id)
                if (min_e < min_child or max_e > max_cc) and c_e[1]['rel_name'] not in ('obl'):
                    new_G['E'].loc[
                        (new_G['E']['id'] == e_id) & (new_G['E']['sentence_id'] == sentence_id), 'source'] = cc
            #         new_G['E'].loc[(new_G['E']['source'] == child) &  (new_G['E']['rel_name'] != 'conj') & ((new_G['E']['target']<child) | (new_G['E']['target']>cc)) & (new_G['E']['state'] == 1), "source"] = cc
            new_G['E'] = new_G['E'].append(new_e, ignore_index=True)
        # print(root_conj)
        root_e_in = new_G['E'][(new_G['E']['target'] == root_conj) & (new_G['E']['state'] == 1) &
                               (new_G['E']['sentence_id'] == sentence_id)]['rel_name'].array[0]
        root_neigh_in = new_G['E'][(new_G['E']['target'] == root_conj) &
                                   (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1)][
            'source'].array[0]
        new_e = {'sentence_id': sentence_id, 'id': len(new_G['E']), 'source': root_neigh_in, 'target': cc,
                 'rel_name': root_e_in, 'state': 1}
        new_G['E'] = new_G['E'].append(new_e, ignore_index=True)
        new_G['E'].loc[(new_G['E']['target'] == root_conj) &
                       (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1), "state"] = 0
        root_edges = new_G['E'][(new_G['E']['source'] == root_conj) & (new_G['E']['rel_name'] != 'conj') &
                                (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1)][
            ['id', 'rel_name', 'target']]
        for r_e in root_edges.iterrows():
            e_id = r_e[1]['id']
            e_target = r_e[1]['target']
            min_e = \
                new_G['V'][(new_G['V']['id'] == e_target) & (new_G['V']['sentence_id'] == sentence_id)]['min'].array[0]
            max_e = \
                new_G['V'][(new_G['V']['id'] == e_target) & (new_G['V']['sentence_id'] == sentence_id)]['max'].array[0]
            # print(e_id, r_e[1]['rel_name'], (min_e < min_rconj or max_e > max_cc) and r_e[1]['rel_name'] not in ('obl'))
            if (min_e < min_rconj or max_e > max_cc) and r_e[1]['rel_name'] not in ('obl'):
                new_G['E'].loc[(new_G['E']['id'] == e_id) & (new_G['E']['sentence_id'] == sentence_id), 'source'] = cc
        #     new_G['E'].loc[(new_G['E']['source'] == root_conj) &  (new_G['E']['rel_name'] != 'conj') & ((new_G['E']['target']<root_conj) | (new_G['E']['target']>cc)) & (new_G['E']['state'] == 1), "source"] = cc
        new_e = {'sentence_id': sentence_id, 'id': len(new_G['E']), 'source': cc, 'target': root_conj,
                 'rel_name': 'conj', 'state': 1}
        new_G['E'] = new_G['E'].append(new_e, ignore_index=True)
    cconjs = new_G['E'][(new_G['E']['rel_name'] == 'conj') & (new_G['E']['state'] == 1)][
        ['source', 'sentence_id']].drop_duplicates()
    for cconj in cconjs.iterrows():
        sentence_id = cconj[1]['sentence_id']
        cconj_id = cconj[1]['source']
        cconj_type = \
        new_G['V'][(new_G['V']['id'] == cconj_id) & (new_G['V']['sentence_id'] == sentence_id)]['type'].array[0]
        if cconj_type != 'cconj':
            new_vid = len(new_G['V'])
            new_v = {'sentence_id': sentence_id, 'id': new_vid, 'text': 'other', 'type': 'cconj',
                     'min': -1, 'max': -1}
            new_G['V'] = new_G['V'].append(new_v, ignore_index=True)
            new_G['E'].loc[
                (new_G['E']['target'] == cconj_id) & (new_G['E']['sentence_id'] == sentence_id) & (
                            new_G['E']['state'] == 1), 'target'] = new_vid
            new_e = {'sentence_id': sentence_id, 'id': len(new_G['E']), 'source': new_vid, 'target': cconj_id,
                     'rel_name': 'conj', 'state': 1}
            new_G['E'] = new_G['E'].append(new_e, ignore_index=True)
            new_G['E'].loc[(new_G['E']['source'] == cconj_id) & (new_G['E']['rel_name'] == 'conj') & (
                    new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1), 'source'] = new_vid
    cases = new_G['E'][(new_G['E']['rel_name'].isin(['case', 'mark'])) & (new_G['E']['state'] == 1)][
        ['id', 'source', 'target', 'sentence_id']]
    for case in cases.iterrows():
        source = case[1]['source']
        target = case[1]['target']
        sentence_id = case[1]['sentence_id']
        id = case[1]['id']
        new_G['E'].loc[(new_G['E']['target'] == source) & (new_G['E']['rel_name'] != 'case') &
                       (new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['state'] == 1), "target"] = target
        new_G['E'].loc[(new_G['E']['id'] == id) & (new_G['E']['sentence_id'] == sentence_id), "source"] = target
        new_G['E'].loc[(new_G['E']['id'] == id) & (new_G['E']['sentence_id'] == sentence_id), "target"] = source
    nsubjs = new_G['E'][new_G['E']['rel_name'].str.contains('nsubj', regex=True) & (new_G['E']['state'] == 1)][
        ['sentence_id', 'id', 'rel_name', 'source', 'target']]
    nsubj_done = []
    for nsubj in nsubjs.iterrows():
        source = nsubj[1]['source']
        rel_name = nsubj[1]['rel_name']
        target = nsubj[1]['target']
        sentence_id = nsubj[1]['sentence_id']
        nsubj_id = nsubj[1]['id']
        # print(source, rel_name, target)
        # print(target, new_rel_name, source)
        # print(source, new_G['V'][(new_G['V']['id'] == source) & (new_G['V']['sentence_id'] == sentence_id)]['text'].array[
        #     0], new_G['V'][(new_G['V']['id'] == source) & (new_G['V']['sentence_id'] == sentence_id)]['type'].array[
        #     0])
        if source in nsubj_done:
            continue
        elif new_G['V'][(new_G['V']['id'] == source) & (new_G['V']['sentence_id'] == sentence_id)]['type'].array[
            0] == 'verb':
            new_rel_name = re.sub('nsubj', 'act_type', rel_name)
            new_G['E'].loc[(new_G['E']['target'] == source) & (new_G['E']['state'] == 1) & (
                    new_G['E']['sentence_id'] == sentence_id), "target"] = target
            new_G['E'].loc[
                (new_G['E']['id'] == nsubj_id) & (new_G['E']['sentence_id'] == sentence_id), "source"] = target
            new_G['E'].loc[
                (new_G['E']['id'] == nsubj_id) & (new_G['E']['sentence_id'] == sentence_id), "target"] = source
            new_G['E'].loc[
                (new_G['E']['id'] == nsubj_id) & (new_G['E']['sentence_id'] == sentence_id), "rel_name"] = new_rel_name
            nsubj_done.append(source)
    print(new_G['V'][['sentence_id', 'id', 'text', 'type']])
    print(new_G['E'][new_G['E']['state'] == 1])
    conditions = new_G['V'][new_G['V']['text'].isin(['yang', 'Dalam hal', 'dalam hal'])]
    for condition in conditions.iterrows():
        id = condition[1]['id']
        sentence_id = condition[1]['sentence_id']
        new_G['E'].loc[
            (new_G['E']['target'] == id) & (new_G['E']['sentence_id'] == sentence_id), 'rel_name'] = 'condition'
        new_G['V'].loc[
            (new_G['V']['id'] == id) & (new_G['E']['sentence_id'] == sentence_id), 'text'] = 'act'
    new_G['V'].loc[new_G['V']['text'] == 'sebagaimana dimaksud', 'text'] = 'refer_to'

    cases = new_G['E'][(new_G['E']['rel_name'].isin(['case', 'mark'])) & (new_G['E']['state'] == 1)][
        ['id', 'source', 'sentence_id', 'target']]
    # print(new_G['V'][['sentence_id', 'id', 'text', 'type']])
    # print(new_G['E'][new_G['E']['state'] == 1])
    for case in cases.iterrows():
        case_word = case[1]['source']
        sentence_id = case[1]['sentence_id']
        case_next = case[1]['target']
        case_id = case[1]['id']
        text_next = new_G['V'][(new_G['V']['id'] == case_next) &
                               (new_G['V']['sentence_id'] == sentence_id)]['text'].array[0]
        try:
            case_prev = new_G['E'][(new_G['E']['target'] == case_word) & (new_G['E']['sentence_id'] == sentence_id) &
                                   (new_G['E']['state'] == 1)]['source'].array[0]
        except Exception:
            continue
        if new_G['V'][(new_G['V']['id'] == case_prev) &
                      (new_G['V']['sentence_id'] == sentence_id)]['text'].array[0] == 'refer_to' and \
                re.search('^(pasal|ayat|huruf|undang-undang|peraturan)', text_next.lower()):
            # print('ccc', case_id)
            # case_prev = new_G['E'][(new_G['E']['source'] == case_word) & (new_G['E']['sentence_id'] == sentence_id) &
            #                        (new_G['E']['state'] == 1)]['source'].array[0]
            refer_to_prev = new_G['E'][(new_G['E']['target'] == case_prev) &
                                       (new_G['E']['sentence_id'] == sentence_id)].sort_values('state',
                                                                                               ascending=False)[
                'source'].array[0]
            new_G['E'].loc[(new_G['E']['source'] == refer_to_prev) & (
                    new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['target'] == case_prev), 'state'] = 0
            new_G['E'].loc[(new_G['E']['source'] == case_prev) & (
                    new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['target'] == case_word), 'state'] = 0
            new_G['E'].loc[(new_G['E']['id'] == case_id) & (
                    new_G['E']['sentence_id'] == sentence_id), 'rel_name'] = 'refer_to'
            new_G['E'].loc[(new_G['E']['id'] == case_id) & (
                    new_G['E']['sentence_id'] == sentence_id), 'source'] = refer_to_prev
            new_G['E'].loc[(new_G['E']['source'] == case_prev) & (
                    new_G['E']['sentence_id'] == sentence_id) & (
                               ~(new_G['E']['target'] == case_word)), 'source'] = refer_to_prev
        else:
            text_case = \
                new_G['V'][(new_G['V']['id'] == case_word) & (new_G['V']['sentence_id'] == sentence_id)]['text'].array[
                    0]
            # print(text_case)
            new_G['E'].loc[(new_G['E']['source'] == case_prev) & (
                    new_G['E']['sentence_id'] == sentence_id) & (new_G['E']['target'] == case_word), 'state'] = 0
            new_G['E'].loc[(new_G['E']['source'] == case_word) & (
                    new_G['E']['sentence_id'] == sentence_id), 'rel_name'] = text_case
            new_G['E'].loc[(new_G['E']['source'] == case_word) & (
                    new_G['E']['sentence_id'] == sentence_id), 'source'] = case_prev
    act_types = new_G['E'][(new_G['E']['rel_name'].isin(['act_type', 'act_type:pass'])) & (new_G['E']['state'] == 1)]
    for act_type in act_types.iterrows():
        source = act_type[1]['source']
        target = act_type[1]['target']
        act_id = act_type[1]['id']
        sentence_id = act_type[1]['sentence_id']
        source_text = \
            new_G['V'][(new_G['V']['id'] == source) & (new_G['V']['sentence_id'] == sentence_id)]['text'].array[0]
        if source_text != 'act':
            source_prev = \
                new_G['E'][(new_G['E']['target'] == source) & (new_G['E']['sentence_id'] == sentence_id) & (
                        new_G['E']['state'] == 1)]['source'].array[0]
            source_in = \
                new_G['E'][(new_G['E']['target'] == source) & (new_G['E']['sentence_id'] == sentence_id) & (
                        new_G['E']['state'] == 1)]['id'].array[0]
            new_vid = len(new_G['V'][new_G['V']['sentence_id'] == sentence_id])
            new_v = {'sentence_id': sentence_id, 'id': new_vid, 'text': 'act', 'type': 'pron',
                     'min': -1, 'max': -1}
            # print(new_v)
            # print(source_prev, source, target)
            new_G['V'] = new_G['V'].append(new_v, ignore_index=True)
            new_e = {'sentence_id': sentence_id, 'id': len(new_G['E'][new_G['E']['sentence_id'] == sentence_id]),
                     'source': new_vid, 'target': source,
                     'rel_name': 'subject', 'state': 1}
            # print(act_id, source_in)
            # print(new_e)

            new_G['E'] = new_G['E'].append(new_e, ignore_index=True)
            new_G['E'].loc[
                (new_G['E']['id'] == source_in) & (new_G['E']['sentence_id'] == sentence_id), 'target'] = new_vid
            new_G['E'].loc[
                (new_G['E']['id'] == act_id) & (new_G['E']['sentence_id'] == sentence_id), 'source'] = new_vid
    # print(new_G['V'])
    # print(new_G['E'][new_G['E']['state'] == 1])
    conjs = new_G['E'][(new_G['E']['rel_name'] == 'conj') & (new_G['E']['state'] == 1)]
    # new_G['V'].loc[new_G['V']['text'] == 'dan/atau', 'text'] = 'or'
    # new_G['V'].loc[new_G['V']['text'] == 'dan', 'text'] = 'and'
    # new_G['V'].loc[new_G['V']['text'] == 'atau', 'text'] = 'xor'
    convert = {'dan/atau': 'or', 'dan': 'and', 'atau': 'xor', 'serta': 'and', 'other': 'uconj'}
    for conj in conjs.iterrows():
        source = conj[1]['source']
        conj_id = conj[1]['id']
        sentence_id = conj[1]['sentence_id']
        print(source, conj_id, sentence_id)
        conj_text = new_G['V'][(new_G['V']['id'] == source) & (new_G['V']['sentence_id'] == sentence_id)]['text'].array[
            0]
        conj_type = new_G['V'][(new_G['V']['id'] == source) & (new_G['V']['sentence_id'] == sentence_id)]['type'].array[
            0]
        new_G['E'].loc[
            (new_G['E']['id'] == conj_id) & (new_G['E']['sentence_id'] == sentence_id), 'rel_name'] = 'subject_' + \
                                                                                                      convert[conj_text]

    new_G['V'].loc[new_G['V']['type'] == 'cconj', 'text'] = 'concept'
    # for nsubj in nsubjs.iterrows():
    #     print(nsubj)
    v = new_G['V']
    e = new_G['E']
    print(v)
    # print(e)
    xx = sqldf('''
                select
                    e.id,
                    sources.text,
                    rel_name,
                    targets.text,
                    sources.id,
                    targets.id
                from
                    e
                join
                    v as sources
                on
                    sources.id = e.source
                join
                    v as targets
                on
                    targets.id = e.target
                where state=1
            ''')
    # print(v)
    print(xx)
    if stop:
        return stop
    # print(nodes)
    # print(edges)


file_list = open('C:/Users/ningg/PycharmProjects/crawling/crawl/pdf_list', 'r+')

state = True
while state:
    text = file_list.readline()
    print(text)
    if text == '':
        state = False
    filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_parsed_files/' + (
        re.sub('_pdf', '.json', re.sub('(%20)|([\\._]+)', '_',
                                       re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/', '',
                                              text)))).lower().strip()
    filetarget = 'E:/Ninggar/Mgstr/Penelitian/Data/files/sentence_triple/' + (
        re.search(r'(.{,100})', re.sub('_pdf', '', re.sub('(%20)|([\\._]+)', '_',
                                                          re.sub('^http(s?)://peraturan\\.go\\.id/common/dokumen/',
                                                                 '', text)))).group(1).lower()) + '.ttl'
    dirName = re.sub('/[^/]+$', '', filetarget)
    try:
        # Create target Directory
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    except Exception:
        None
    try:
        # filesource = 'E:/Ninggar/Mgstr/Penelitian/Data/files/new_1_text_files/perda/2019/perwal_no_5_tahun_2019.txt'
        file = json.load(open(filesource, 'r'))
    except Exception:
        continue
    if re.search('putusan', filesource):
        continue
    print(file['body'].keys())
    if re.search(r'^perubahan\s\w+\satas\s+(peraturan|undang)', file['title']['name'].lower()):
        continue
    start = time.time()
    # time.microsecond
    get_body_values(file['body'], 'chapters')
    end = time.time()
    print(end - start)
    break
