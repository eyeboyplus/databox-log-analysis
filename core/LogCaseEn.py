#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import datetime
import sys

import pymongo
import redis

from core.databox_global import DataBoxGlobal
from core import MedicalDataGraphEn

# on docker
databox = DataBoxGlobal()
config_path = databox.get_relative_path('conf/conf')
localhost = '127.0.0.1'
# on my pc
# config_path = './file/conf'
# localhost = '127.0.0.1'
mongo_db_name = 'LogCaseEn'
# 设置相似序列数组重复度
simBase = 0.8
#sensitive_table_group = [['Doctor', 'Drug']]

# 每个医生能被获取的开药的信息只能被拿到30%
threshold = 0.3

tableInfoDict = {'cc': ['medical_record_id', 'type', 'sex', 'age', 'name','cc_id','time','applicant'],
                 'jyzb':['jyzb_id','cc_id', 'jyzb_code', 'ref_range', 'res', 'item','tip'],
                 'medical_record': ['medical_record_id','room', 'patient_id','ICD', 'time', 'doctor_id'],
                 'patient': ['patient_id', 'name','sex','age','addr'],
                 'doctor': ['doctor_id', 'name','sex','age','addr','room'],
                 'prescription':['prescription_id','drug','medical_record_id','dose'],
                 'jyzb_info': ['jyzb_code','jyzb_name','jyzb_ab','unit','male','female','baby'],
                 'diagnosis': ['TSH', 'FT3', 'FT4']
                 }

tablePKDict = {
                 'cc': "cc_id",
                 'jyzb': "jyzb_id",
                 'medical_record': "medical_record_id",
                 'patient': "patient_id",
                 'doctor': "doctor_id",
                 'prescription': "prescription_id",
}

class LogCase:
    def __init__(self, db_name="LogCaseEn"):
        self.mongo_conn = pymongo.MongoClient(localhost, 27017)
        self.mongo_db = self.mongo_conn[db_name]
        #  log存储表
        self.log_table = self.mongo_db.LogInfo
        self.log_conf_table = self.mongo_db.LogConf
        self.time_table = self.mongo_db.TimeInfo
        # db 0 记录数据控制相关中间结果---关联数据信息
        self.relation_redisDB = redis.Redis(host=localhost, port=6379, db=0)

        self.table_graph = MedicalDataGraphEn.MedicalDataGraph()
        self.pk_dict = tablePKDict

        self.redis_name_dict = get_db_conf(config_path)

    # log_conf信息初始化
    def conf_data_initial(self):
        data_mongo = self.mongo_conn['DataEn']
        # for table in self.table_graph.G.nodes_iter():
        for table in self.table_graph.G.nodes():
            data_size = data_mongo[table].find().count()
            print(table, data_size)
            for service in self.redis_name_dict.keys():
                self.log_conf_table.insert({'serviceName': service, 'col': table, 'dataSize': data_size,
                                            'safeSG': 0.01, 'exposedDataSG': 0})

    def convert(self, col, fields):
        res = ""
        for f in tableInfoDict[col]:
            res += "1" if f in fields else "0"
        return res

    # 获取已泄露的字段所代表的二进制string
    # return "11011" | "10010"
    def cal(self, v1, v2):
        if v1 is None:
            return v2
        elif v2 is None:
            return v1
        elif len(v1) != len(v2):
            print ("the length of v1 and v2 is not equal")
            return ""
        else:
            l = len(v1)
            res = ""
            for i in range(l):
                res += str(int(v1[i])|int(v2[i]))
            return res

    # 判断该条记录是否被访问所有字段（被泄露）
    def checkFields(self,val):
        return b"0" not in val
        # 服务请求被允许后，进行相应更新操作
    def update_log_detail(self,logid,  col_conf, service_name, col_name, field_list, fk_info, data_list):
        # 根据serviceName获取当前服务对应redisDB.
        data_redis = redis.Redis(host=localhost, port=6379, db=self.redis_name_dict[service_name])
        data_pipeline = data_redis.pipeline()
        relation_pipeline = self.relation_redisDB.pipeline()
        pastvalue_list = {}
        field_str = self.convert(col_name, field_list)
        t1 = datetime.datetime.now()
        for data in data_list:
            # 历史访问字段获取
            pastvalue = data_redis.hget(col_name + "detail", data)
            pastvalue_list[data] = pastvalue

        for data in data_list:
            # 更新数据访问次数
            # 进行访问频次更新，每被访问一次增加一次次数
            data_pipeline.hincrby(col_name, data, 1)
            newvalue = self.cal(pastvalue_list[data], field_str)
            data_pipeline.hset(col_name + "_detail", data, newvalue)

        hl = 0
        for k in data_redis.hkeys(col_name + "_detail"):
            if self.checkFields(data_redis.hget(col_name + "_detail",k)):
                hl+=1
        data_pipeline.execute()
        #relation_pipeline.execute()
        col_exposed_sg = hl / col_conf['dataSize']
        t2 = datetime.datetime.now()
        delta_t1 = t2 - t1


        relation_update_info = {}
        i = 0
        # 获取相关旧关系，并生成新关系
        for fk in fk_info.keys():
            fk_list = fk_info[fk]
            relation_name = col_name+ '->' + self.table_graph.get_node_by_pk(fk)
            relation_update_info[relation_name] = {}
            fk_id = fk_list[i]
            pk_id = data_list[i]
            i+=1
            pastv = self.relation_redisDB.hget(relation_name, pk_id)
            if pastv is not None and fk_id in pastv:
                newv = pastv + [fk_id]
                relation_update_info[relation_name][pk_id]=newv

        for rn in relation_update_info.keys():
            for pid in relation_update_info[rn].keys():
                relation_pipeline.hset(rn,pid,relation_update_info[rn][pid])
        relation_pipeline.execute()

        t3 = datetime.datetime.now()
        delta_t2 = t3 - t2
        self.time_table.insert({'cost1':delta_t1.seconds + delta_t1.microseconds/1000000.0,
                                "cost2":delta_t2.seconds + delta_t2.microseconds/1000000.0,
                                'time':datetime.datetime.now(),'log_id':logid})
        self.log_conf_table.update({'serviceName': service_name.lower(), 'col': col_name},
                                   {'$set': {'exposedDataSG': col_exposed_sg}}, False, False)


    # True：允许服务请求；False：拒绝服务请求
    # log中{relFields:[F1,F2,F3…..]}
    # 假设data格式为{[f1,f2],[f1,f2]....}
    # 一个col的f1数据 ： data[i][log['relFields'].index(f1)]
    def sensitive_check(self, log):
        log_dict = eval(log)
        col_name = log_dict['relCol'].strip()
        field_list = log_dict['relFields']

        sensitive_paths = self.table_graph.in_sensitive_path(col_name)
        # 判断当前服务涉及的表是否在需要控制的两个表的某条路径上

        if len(sensitive_paths) == 0:
            return []

        # 判断当前服务涉及的数据是否包含外键
        sensitive_fields, visited_sensitive_paths = self.table_graph.has_sensitive_fields(col_name,sensitive_paths, field_list)

        return sensitive_fields

    # 对合并sensitive_check和service_control两个功能的支持
    def m_sensitive_check(self, log):
        sensitive_fields = self.sensitive_check(log)
        log_dict = eval(log)
        if len(sensitive_fields) <= 0:
            log_dict['fkList'] = {}
        return str(log_dict)

    '''
    根据conf信息，对服务进行初步控制
    '''
    def service_control(self, log):
        #print(log)
        res_flag = False
        #log_dict = log
        log_dict = eval(log)
        service_name = log_dict['serviceName']
        col_name = log_dict['relCol']
        #col_conf = self.log_conf_table.find_one({'col': col_name, 'serviceName': service_name},{"_id":0})
        col_conf = self.log_conf_table.find_one({'col': col_name, 'serviceName': service_name})
        fk_list = log_dict['fkList']
        # 存在外键列表时，证明当前log涉及敏感数据，标识为isSensitive

        log_dict['isSensitive'] = str(len(fk_list) > 0).lower()

        if str(service_name).lower() not in self.redis_name_dict.keys():
            res_flag = True
        elif col_conf is not None and col_conf['exposedDataSG'] >= col_conf['safeSG']:
            res_flag = False
        # 如果没有超过配置文件中的safeSG,进行再一步判断
        else:
            res_flag = True
            # 序列 & 关联关系判断
            # 序列判断：serviceName相同的log，bson类似=> 使用ArraySimilarity判断重合度，过高拒绝。
            #

            #res_flag, fk_list = self.service_control_detail(log_dict, col_conf)

        # 根据请求是否被允许标记isAllowed字段，并将此log存入LogInfo
        log_dict['isAllowed'] = str(res_flag).lower()
        logid = self.log_table.insert(log_dict)

        #print(log_dict)
        # 服务请求被允许，进行log detail 更新操作
        if res_flag and str(service_name) in self.redis_name_dict.keys():
            self.update_log_detail(logid, col_conf, service_name, col_name,log_dict['relFields'],
                                   fk_list, log_dict['relDataList'])
        return res_flag

    def initial_log_conf(self, service_name, col, data_size, safe_sg, exposed_data_size):
        self.log_conf_table.insert({'serviceName': service_name,'col': col, 'dataSize': data_size,
                                    'safeSG': safe_sg, 'exposedDataSG': exposed_data_size / data_size})

    def update_log_conf(self, service_name, col, safe_sg):
        self.log_conf_table.update({'serviceName': service_name, 'col': col},
                                   {'$set': {'safeSG': safe_sg}}, False, False)

    '''
    清空mongoDB和redis数据
    '''
    def clear(self):
        r = redis.Redis(host=localhost, port=6379, db=0)
        r.flushall()
        self.mongo_db.drop_collection('LogConf')
        self.mongo_db.drop_collection('LogInfo')
        self.mongo_db.drop_collection('TimeInfo')

    # 合并sensitive_check和service_control两个功能
    def mix_control(self, log):
        new_log = self.m_sensitive_check(log)
        self.service_control(new_log)


def get_db_conf(config_file):
    res_dict = {}
    file_object = open(config_file, 'r')
    try:
        for line in file_object:
            info = line.split(' ')
            res_dict[info[0].strip()] = int(info[1].strip()) + 1
    except:
        print('Read DB-info CONFIG FILE failed.')
    finally:
        file_object.close()
    return res_dict

if __name__ == '__main__':
    for i in range(0, len(sys.argv)):
        print(i, sys.argv[i])

    lg = LogCase('LogCase')
    func_type = str(sys.argv[1]).strip()

    print('log:', sys.argv[2])
    if func_type == 'initial':
        lg.clear()
        lg.conf_data_initial()
        print('successfully initialed')
    # elif func_type == 'sensitive_check':
    #     print(lg.sensitive_check(sys.argv[2]))
    # elif func_type == 'service_control':
    #     # print('enter service_control',sys.argv[2])
    #     print(lg.service_control(sys.argv[2]))
    elif func_type == 'control':
        lg.mix_control(sys.argv[2])
    else:
        print('func_type error')
