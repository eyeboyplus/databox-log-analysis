

class HandledLog:
    def __init__(self, log, is_sensitive, is_allowed):
        self.serviceName = log.serviceName

        self.relCol = log.relCol
        self.relFields = log.relFields
        self.relDataList = log.relDataList
        self.fkList = log.fkList

        self.createdTime = log.createdTime

        self.isSensitive = is_sensitive
        self.isAllowed = is_allowed
