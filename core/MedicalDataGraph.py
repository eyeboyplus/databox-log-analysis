#!/usr/bin/python
# -*- coding: UTF-8 -*-
import networkx as nx
from bidict import bidict

from core import XMLReader
from core.databox_global import DataBoxGlobal

# tableInfoDict = {'cc': ['medical_record_id', 'type', 'sex', 'age', 'name','cc_id','time','applicant'],
#                  'jyzb':['jyzb_id','cc_id', 'jyzb_code', 'ref_range', 'res', 'item','tip'],
#                  'medical_record': ['medical_record_id','room', 'patient_id','ICD', 'time', 'doctor_id'],
#                  'patient': ['patient_id', 'name','sex','age','addr'],
#                  'doctor': ['doctor_id', 'name','sex','age','addr','room'],
#                  'prescription':['prescription_id','drug','medical_record_id','dose'],
#                  'jyzb_info': ['jyzb_code','jyzb_name','jyzb_ab','unit','male','female','baby'],
#                  'diagnosis' : ['TSH', 'FT3', 'FT4']
#                  }
#
# tablePKDict = {
#                  'cc': "cc_id",
#                  'jyzb':"jyzb_id",
#                  'medical_record': "medical_record_id",
#                  'patient':"patient_id",
#                  'doctor': "doctor_id",
#                  'prescription':"prescription_id",
#                  'diagnosis': 'TSH'
# }
#
# tableFkDict = [
#     ['doctor', 'medical_record', 'doctor_id'],
#     ['medical_record', 'patient', 'patient_id'],
#     ['medical_record', 'prescription', 'medical_record_id'],
#     ['cc', 'patient', 'medical_record_id'],
#     ['jyzb', 'cc', 'cc_id']
# ]


databox = DataBoxGlobal()
# table_meta = databox.get_relative_path('conf/DataMeta.xml')
# relation_meta = databox.get_relative_path('conf/RelationMeta')
# sensitive_meta = databox.get_relative_path('conf/Protection.xml')


class MedicalDataGraph:
    def __init__(self, tableInfoDict, tablePKDict, tableFKDict, relationProtectionList):
        self.G = nx.Graph()
        self.__info_initial(tableInfoDict, tablePKDict, tableFKDict)
        nx.connected_components(self.G)
        self.__sensitive_meta_initial(relationProtectionList)

    def __info_initial(self, tableInfoDict, tablePKDict, tableFKDict):
        for t in tablePKDict.keys():
            self.G.add_node(t, pk=tablePKDict[t],fields=tableInfoDict[t])
        for f in tableFKDict:
            self.G.add_edge(f[0],f[1],fk=f[2])

    def __sensitive_meta_initial(self, relationProtectionList):
        self.STG = []
        for relation in relationProtectionList:
            self.STG.append((relation[0],relation[1]))

    # 给出两个节点中的所有路径
    def all_simple_paths(self, node1, node2):
        return nx.all_simple_paths(self.G, node1, node2)

    # 生成主键和表对应字典
    def get_pk_dict(self):
        pk_dict = {}
        for node in self.G.nodes():
            pk_dict[node] = self.G.node[node]['pk']
        return pk_dict

    # 给出一条路径上的所有外键
    def get_edge_info_of_path(self, path):
        fk_list = []
        for ind in range(len(path)-1):
            fk_list += self.G.get_edge_data(path[ind], path[ind+1])['fk'],
        return fk_list

    # 获取某个表的主键
    def get_node_pk(self, node):
        return self.G.node[node]['pk']

    def get_node_by_pk(self,pk):
        for node in self.G.nodes():
            if self.G.node[node]['pk'] == pk:
                return node
        return None

    # 获取当前path各节点的主键字典
    # {主键：对应表}
    def get_pk_dict_of_path(self, path):
        pk_dict = {}
        for node in path:
            pk_dict[self.get_node_pk(node)] = node
        return pk_dict

    # 给出当前路径相关的表查询条件状语 以及 最终查询结果需要的path上的各节点所代表的表的主键列表
    def get_sql_info_of_path(self, path):
        if len(path) is 0:
            return [], []
        cond_list = []
        pk_list = [path[0] + '.' + self.get_node_pk(path[0])]
        for ind in range(len(path)-1):
            fk = self.G.get_edge_data(path[ind], path[ind+1])['fk']
            cond_list += path[ind] + '.' + fk + ' = ' + path[ind+1] + '.' + fk,
            pk_list += path[ind+1] + '.' + self.get_node_pk(path[ind+1]),
        return cond_list, pk_list

    # 获取已知一边（两个node信息）的情况下，需要补充的node信息
    # path: ['Drug', 'Prescription', 'MedicalRecord', 'Doctor']
    # pk_node:'Prescription'
    # fk_node:'MedicalRecord'
    # 返回：['Prescription', 'Drug'], ['MedicalRecord', 'Doctor']
    def get_rel_edges(self, path, pk_node, fk_node):
        pk_ind = path.index(pk_node)
        fk_ind = path.index(fk_node)
        if pk_ind < fk_ind:
            return path[:pk_ind+1][::-1], path[fk_ind:]
        else:
            return path[:fk_ind+1][::-1], path[pk_ind:]



    # 判断node是否在node1和node2的路径上，并返回所有包含node的路径
    def __in_path(self, node, node1, node2):
        paths = []
        for path in nx.all_simple_paths(self.G, node1, node2):
            if node in path:
                paths += path,
        return paths

    # 获取某个表在某条路径上相关的外键列表
    def __get_node_fk_in_path(self, path, node):
        # 获取该表在路径上的位置
        node_ind = path.index(node)
        res = []
        # 根据位置获取该表在路径上前后两条边的外键数据
        if node_ind > 0:
            res += self.G.get_edge_data(node, path[node_ind-1])['fk'],
        if node_ind < len(path) - 1:
            res += self.G.get_edge_data(node, path[node_ind+1])['fk'],
        # 去除该表的主键本身
        node_pk = self.get_node_pk(node)
        if node_pk in res:
            res.remove(node_pk)
        # 返回去重结果
        return list(set(res))

    # 判断当前节点node是否在需要目标表的关联路径上并返回涉及的目标表
    def in_sensitive_path(self,node):
        sensitive_paths = []
        for tg in self.STG:
            sensitive_paths += self.__in_path(node, tg[0], tg[1])
        return sensitive_paths

    # 判断当前服务涉及的数据是否包含外键
    # 返回：是否包含外键， 外键列表
    # def has_sensitive_fields(self, node, field_list):
    #     flag = False
    #     fk_list = []
    #     for e in self.G.edges(node, data='fk'):
    #         if e[2] in field_list and e[2] is not self.G.node[node]['pk']:
    #             flag = True
    #             fk_list += e[2],
    #     return flag, fk_list

    def has_sensitive_fields(self, node, sensitive_paths, field_list):
        sensitive_fields = []
        visited_sensitive_paths = []
        for p in sensitive_paths:
            # 对每条敏感路径计算数据表相关的外键字段
            sf = self.__get_node_fk_in_path(p, node)
            # 判断访问的字段列表是否涉及该条敏感路径的敏感外键字段
            for f in field_list:
                if f in sf:
                    sensitive_fields += f,
                    visited_sensitive_paths += p,
        return sensitive_fields, visited_sensitive_paths




if __name__ == "__main__":

    mdG = MedicalDataGraph(sensitive_meta)
    print(mdG.get_node_by_pk("doctor_id"))
    print(mdG.G.nodes_iter())
    print(mdG.G.edges('MedicalRecord', data='fk'))
    print(mdG.in_sensitive_path("Prescription"))
    # for e in mdG.G.edges():
    #     print(e)
    for i in mdG.all_simple_paths('Drug', 'Doctor'):
        # print(i)
        # a, b = mdG.get_sql_info_of_path(i)
        # print(mdG.get_edge_info_of_path(i))
        # print("get_node_fk",mdG.get_node_fk_in_path(i, 'Prescription'))
        # print(a)
        # print(b)
        # print(mdG.get_rel_edges(i, 'Prescription','MedicalRecord'))
        # print(mdG.G.edges('MedicalRecord',data='fk'))
        c = bidict(mdG.get_pk_dict())
        print(c)
        print(c.inv['patient_id'])