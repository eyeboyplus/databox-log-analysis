from core import MedicalDataGraph, LogRedis
from core import LogDatabase as logDB
from core import HandledLog as hLog
from core import Log
from core.databox_global import DataBoxGlobal
from core import DatasetMetaXmlReader, ProtectionXmlReader

import pymongo

services_dict = {
    'getData': 1,
    'getAllValue': 2,
    'getAverage': 3,
    'getConditionCount': 4,
    'dataCount': 5,
    'getFilterData': 6,
    'getMax': 7,
    'getMin': 8,
    'getDistinctValue': 9,
    'projections': 10
}

databox = DataBoxGlobal()
dataset_meta_path = databox.get_relative_path('conf/dataset-meta.xml')
protection_path = databox.get_relative_path('conf/protection.xml')
datasetMetaReader = DatasetMetaXmlReader.DatasetMetaXmlReader(dataset_meta_path)
protectionXmlReader = ProtectionXmlReader.ProtectionXmlReader(protection_path)
tableInfoDict = datasetMetaReader.getTableInfoDict()
tablePKDict = datasetMetaReader.getTablePkDict()
tableFKDict = datasetMetaReader.getTableFkDict()
relationProtectionList = protectionXmlReader.getRelationProtectionList()

log_redis_ip = '127.0.0.1'
log_redis_port = 6379

sensitive_table_internal = {
    'diagnosis': []
}


class LogAnalysis:
    def __init__(self):
        self.table_graph = MedicalDataGraph.MedicalDataGraph(tableInfoDict=tableInfoDict,
                                                             tablePKDict=tablePKDict,
                                                             tableFKDict=tableFKDict,
                                                             relationProtectionList=relationProtectionList)
        self.log_db = logDB.LogConfDatabase('127.0.0.1', 27017, 'LogCase')

    def init(self):
        mongo_conn = pymongo.MongoClient('127.0.0.1', 27017)
        data_db = mongo_conn['DataEn']
        self.log_db.clear()
        self.log_db.init(data_db, self.table_graph.G.nodes(), services_dict.keys())

    def analysis(self, log):
        # sensitive check
        col_name = log.relCol
        field_list = log.relFields
        sensitive_paths = self.table_graph.in_sensitive_path(col_name)
        # 不敏感 外键数据置空
        if len(sensitive_paths) == 0:
            log.fkList = {}

        # service control
        res_flag = True
        res = {'status': res_flag, 'info': ''}
        handled_log = hLog.HandledLog(log, 'False', 'False')
        service_name = log.serviceName

        # set isSensitive flag
        fk_list = log.fkList
        handled_log.isSensitive = str(len(fk_list) > 0).lower()

        col_conf = self.log_db.get_log_conf(col_name, service_name)
        if str(service_name).lower() not in services_dict.keys():
            res_flag = True
        elif col_conf is not None and col_conf['exposedDataSG'] >= col_conf['safeSG']:
            res_flag = False
            res['status'] = res_flag
            res['info'] = '数据量超过安全阈值'
        else: # 表内敏感字段判断
            is_table_internal_sensitive = True
            table_internal_sensitive_paths = sensitive_table_internal[col_name]
            for path in table_internal_sensitive_paths:
                is_table_internal_sensitive = True
                for item in path:
                    if item not in field_list:
                        is_table_internal_sensitive = False
                        break
                if is_table_internal_sensitive:
                    res_flag = False
                    res['status'] = res_flag
                    res['info'] = col_name + ' 表间字段敏感'
                    break

        handled_log.isAllowed = str(res_flag).lower()
        # 处理过的log存入LogInfo
        logid = self.log_db.insert_handled_log(handled_log)
        refDataList = log.relDataList

        log_redis = LogRedis.LogRedis(log_redis_ip, log_redis_port,
                                      services_dict[service_name],
                                      col_name)
        # 请求通过
        if res_flag and str(service_name) in services_dict.keys():
            for pk in refDataList:
                # 服务-表-记录 频次计数
                log_redis.increase_frequency(pk)

                # 更新字段访问详细数据
                past_value = log_redis.get_detail_value(pk)
                fields_binary_str = self.__fields_to_binary_string(col_name, field_list)
                new_value = self.__string_or(past_value, fields_binary_str)
                log_redis.update_detail_value(pk, new_value)

        log_redis.submit()
        return res

    # 表的字段是否访问过，如果访问过则该字段至为1，否则为0
    # 形如 110010
    def __fields_to_binary_string(self, col, fields):
        res = ""
        for f in tableInfoDict[col]:
            res += "1" if f in fields else "0"
        return res

    # return "11011" | "10010"
    def __string_or(self, v1, v2):
        if v1 is None or v1.strip() == b'':
            if v2 is None or v2.strip() == b'':
                return ''
            else:
                return v2
        else:
            if v2 is None or v2.strip() == b'':
                return v1
            else:
                len1 = len(v1)
                len2 = len(v2)
                if len1 != len2:
                    return ''
                else:
                    res = ''
                    for i in range(len1):
                        res += str(int(v1[i]) | int(v2[i]))
                    return res


if __name__ == '__main__':
    log_analysis = LogAnalysis()
    log_analysis.init()
    log = Log.Log("{'serviceName':'getData', 'relCol':'diagnosis', 'relFields':['TSH'],"
                  "'relDataList':['1','2'], 'fkList':[],'createdTime':'20180208'}")
    print(log_analysis.analysis(log))
    log = Log.Log("{'serviceName':'getData', 'relCol':'diagnosis', 'relFields':['FT4'],"
                  "'relDataList':['1','2'], 'fkList':[],'createdTime':'20180208'}")
    print(log_analysis.analysis(log))
