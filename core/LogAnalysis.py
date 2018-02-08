from core import MedicalDataGraphEn, LogRedis
from core import LogDatabase as logDB
from core import HandledLog as hLog

import pymongo

service_names = ['getData', 'getAllValue', 'getAverage', 'getConditionCount',
                 'dataCount', 'getFilterData', 'getMax', 'getMin',
                 'getDistinctValue', 'projections']

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

log_redis_ip = '127.0.0.1'
log_redis_port = 6379

sensitive_table_internal = {
    'diagnosis': []
}


class LogAnalysis:
    def __init__(self):
        self.table_graph = MedicalDataGraphEn.MedicalDataGraph()
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
            # 服务.表.记录 频次计数
            for pk in refDataList:
                log_redis.increase_frequency(pk)

        log_redis.submit()
        return res


if __name__ == '__main__':
    log_analysis = LogAnalysis()
    log_analysis.init()
