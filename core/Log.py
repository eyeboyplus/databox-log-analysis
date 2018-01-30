
class Log:
    def __init__(self, json_str):
        log = eval(json_str)

        self.serviceName = log['serviceName']

        self.relCol = log['relCol'].strip()
        self.relFields = log['relFields']
        self.relDataList = log['relDataList']
        self.fkList = log['fkList']

        self.createdTime = log['createdTime']
