#!/usr/bin/python
# -*- coding: UTF-8 -*-
from xml.dom import minidom

def get_ele_text(root_ele, ele_name):
    eles = root_ele.getElementsByTagName(ele_name)
    if len(eles) == 0:
        return ""
    return eles[0].childNodes[0].nodeValue.strip()

def parse_data_meta(filename):
    doc = minidom.parse(filename)
    top_ele = doc.documentElement
    dataset = []
    relations = []
    tables = top_ele.getElementsByTagName('table')
    for ele in tables:
        table = {}
        table['name'] = get_ele_text(ele,'table-name')
        table['pk'] = get_ele_text(ele,'pk')
        for fk_info in ele.getElementsByTagName('fk'):
            relation = {}
            ref_table = get_ele_text(fk_info, 'ref-table')
            relation['tables'] = [table['name'],ref_table]
            relation['fk'] = get_ele_text(fk_info, 'fk-name')
            relations.append(relation)
        dataset.append(table)
    return dataset,relations



path = '../file/DataMeta.xml'

def parse_protection(filename):
    doc = minidom.parse(filename)
    top_ele = doc.documentElement
    protections = {'data_protections':[],'relation_protections':[]}
    data_protections = top_ele.getElementsByTagName('data-protection')[0].\
        getElementsByTagName('table')
    for ele in data_protections:
        dp = {}
        dp['table_name'] = get_ele_text(ele, 'table-name')
        dp['table_size'] = get_ele_text(ele, 'table-size')
        service_protections = []
        for service in ele.getElementsByTagName('service'):
            service_protection = {}
            service_protection['service_name'] = get_ele_text(service,'service-name')
            service_protection['safe_sg'] = get_ele_text(service,'safe-sg')
            service_protection['min_stats_data'] = get_ele_text(service,'min-stats-data')
            service_protections.append(service_protection)
        dp['service_protects'] = service_protections
        protections['data_protections'].append(dp)
    rel_protections = top_ele.getElementsByTagName('relation-protection')[0].\
        getElementsByTagName('relation')
    for ele in rel_protections:
        rf = []
        for rel in ele.getElementsByTagName('table'):
            rf.append(rel.childNodes[0].nodeValue.strip())
        protections['relation_protections'].append(rf)
    return protections

path2 = '../file/Protection.xml'
