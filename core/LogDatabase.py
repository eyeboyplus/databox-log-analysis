import pymongo


class LogConfDatabase:
    def __init__(self, ip, port, db_name):
        self.mongo_conn = pymongo.MongoClient(ip, port)
        self.log_db = self.mongo_conn[db_name]

        self.log_table = self.log_db.LogInfo
        self.log_conf_table = self.log_db.LogConf
        self.time_table = self.log_db.TimeInfo

    # 根据所有数据集的表和服务初始化两者相关的配置
    def init(self, data_db, tables, services):
        for table in tables:
            data_size = data_db[table].find().count()
            for service in services:
                self.log_conf_table.insert({'serviceName': service, 'col': table, 'dataSize': data_size,
                                            'safeSG': 0.01, 'exposedDataSG': 0})

    def get_log_conf(self, col_name, service_name):
        return self.log_conf_table.find_one({'col':col_name, 'serviceName':service_name})

    def insert_handled_log(self, handled_log):
        return self.log_table.insert(handled_log.__dict__)

    def clear(self):
        self.log_db.drop_collection('LogConf')
        self.log_db.drop_collection('LogInfo')
        self.log_db.drop_collection('TimeInfo')
