#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import time
from DataSimulationEn import *
from LogCaseEn import *


tableNameSuffix = 'LogStats'

serviceList = ['projections','getCollections', 'collectionStruct', 'getData', 'getAverage', 'getMax', 'getMin', 'getConditionCount',"getDistinctValue"]
bsonParamList = ['eq', 'ne', 'in', 'nin', 'and', 'or', 'nor', 'not', 'exists', 'gt', 'gte', 'lt', 'lte']

dataSize = 10000000
selectPec = [0.00005, 0.00010, 0.00015, 0.00020, 0.00025, 0.00030]
# 0.005%*10000000 = 500~3000

conn = pymongo.MongoClient('localhost', 27017)
db = conn.LogsEn
logTable = db.LogInfo
statsTable = db.LogStats
statsTestTable = db.LogStatsTest
logConfTable = db.LogConf

jyzb_info = getTable("jyzb_info", {}, {"_id": 0})
jyzb_items = map(lambda x: x["jyzb_name"], jyzb_info)
lc = LogCase()

'''

科室名称 = ['针灸科', '内分泌科', '甲状腺科', '肾内科','泌尿科']
roomList = ['acupuncture', 'endocrinology', 'thyroid', 'nephrology','urology']
性别=["男","女"]
sex=["male","female"]
医生年龄：（25，55）
患者年龄：（1，90）
['甲状腺机能亢进', '甲状腺恶性肿瘤',  '甲状腺炎','unknown']['肾功能衰竭', 'unknown']
['hyperthyrea', 'AMTT', 'thyroiditis','unknown']['kidney','unknown']
就诊类型：['门诊', '住院']
type:['outpatient', 'hospitalized']
指标名称：['正常','偏高','偏低']
res:['normal','high','low']


'jyzb_info': ['jyzb_code','jyzb_name','jyzb_ab','unit','male','female','baby']
'''
tableInfoDict = {'cc': ['cc_id','medical_record_id', 'type', 'sex', 'age', 'name','time','applicant'],
                 'jyzb':['jyzb_id','cc_id', 'jyzb_code', 'ref_range', 'res', 'item','tip'],
                 'medical_record': ['medical_record_id','room', 'patient_id','ICD', 'time', 'doctor_id'],
                 'patient': ['patient_id', 'name','sex','age','addr'],
                 'doctor': ['doctor_id', 'name','sex','age','addr','room'],
                 'prescription':['prescription_id','drug','medical_record_id','dose'],
                 }
tablePKDict = {
                 'cc': "cc_id",
                 'jyzb':"jyzb_id",
                 'medical_record': "medical_record_id",
                 'patient':"patient_id",
                 'doctor': "doctor_id",
                 'prescription':"prescription_id",
}

redis_name_dict = get_db_conf('../file/conf')


'''
* List<Document> prescriptionInfo = projections( ‘诊断’ , [‘就诊流水号’，‘医疗卡号’,’医生工号' ] )
* patientList = getDistinctValue( ‘诊断’ , ‘医疗卡号’)
* medicalRecordList = getDistinctValue( ‘诊断’ , ‘就诊流水号’)
* List<Document> drugInfo = projections( ‘用药’ , ['就诊流水号’, ‘药品名称’ ], Filters.in(‘就诊流水号’, medicalRecordList) )
'''
def generateOneLog1026(serviceName, relCol, relFields, relDataList, createdTime, reqId):
    log = {"serviceName": serviceName, "relCol": relCol, "relFields": relFields,
            "relDataList":relDataList,"createTime": createdTime, 'reqId':reqId }
    col_conf = logConfTable.find_one({'col': relCol, 'serviceName': serviceName})
    if col_conf is not None:
        if col_conf['exposedDataSG'] < col_conf['safeSG']:
            try:
                data_redis = redis.Redis(host=localhost, port=6379, db=redis_name_dict[serviceName.lower()])
                data_pipeline = data_redis.pipeline()
                for id in relDataList:
                    data_pipeline.hincrby(relCol, id, 1)
                data_pipeline.execute()
                col_exposed_sg = data_redis.hlen(relCol) / col_conf['dataSize']

                logConfTable.update({'serviceName': serviceName, 'col': relCol},
                                    {'$set': {'exposedDataSG': col_exposed_sg}}, False, False)
            except:
                print(redis_name_dict)
                print(relCol)
            log["isAllowed"] = "true"
        else:
            log["isAllowed"] = "false"
    else:
        log["isAllowed"] = "true"

    logTable.insert(log)
    #return log

def generateOneLog1028(service, relCol, relFields, reqId):
    colFields = tableInfoDict[relCol]
    pk = tablePKDict[relCol]
    if pk not in relFields:
        relFields += pk,
    filt = generateFilter(relCol)
    fcond = {}
    for f in relFields:
        fcond[f] = 1

    info = getTable(relCol, filt, fcond, False, 0.0005)

    if len(info) == 0:
        idList = []
        fkList = []
    else:
        try:
            idList = list(set(map(lambda x: x["就诊流水号"], info)))
            fkList = list(set(map(lambda x: x["医生工号"], info)))

        except:
            idList = list(set(map(lambda x: x[u"就诊流水号"], info)))
            fkList = list(set(map(lambda x: x[u"医生工号"], info)))

    log = {"serviceName": service, "relCol": relCol, "relFields": relFields,
       "relDataList": idList, "createTime": "2017-05-29 15:06:23", 'reqId': reqId,"fkList":{"医生工号":fkList}}

    log["isSensitive"] = "true"
    log["isAllowed"] = "true"

    logTable.insert(log)


def generateSeq(a,b,c,d,seqId):
    prescInfo = getTable2("medical_record", {"ICD": {"$in": ['hyperthyrea', 'AMTT', 'thyroiditis']}}, {"medical_record_id": 1, "patient_id": 1, "doctor_id": 1},False, 0.01)
    patientInfo = list(set(map(lambda x: x["doctor_id"], prescInfo)))
    mrInfo = list(set(map(lambda x: x["medical_record_id"], prescInfo)))
    drugInfo = getTable2("prescription", {"medical_record_id": {"$in": mrInfo}}, {"prescription_id":1 ,"medical_record_id": 1, "drug": 1}, False,0.01)
    drugList = list(set(map(lambda x:x["prescription_id"],drugInfo)))

    log1 = {"serviceName": "projections", "relCol": "medical_record", "relFields": ["medical_record_id","patient_id","doctor_id"],
           "relDataList": mrInfo, "createTime": a, 'reqId': seqId,
           'bson': str({"ICD": {"$in": ['hyperthyrea', 'AMTT', 'thyroiditis']}}), "fkList": []}
    log2 = {"serviceName": "getDistinctValue", "relCol": "medical_record", "relFields": ["patient_id"],
           "relDataList": mrInfo, "createTime": b, 'reqId': seqId,
           'bson': {}, "fkList": []}
    log3 = {"serviceName": "getDistinctValue", "relCol": "medical_record", "relFields": ["medical_record_id"],
           "relDataList": mrInfo, "createTime": c, 'reqId': seqId,
           'bson': {}, "fkList": []}
    log4 = {"serviceName": "projections", "relCol": "prescription", "relFields": ["medical_record_id","drug"],
           "relDataList": drugList, "createTime": d, 'reqId': seqId,
           'bson': str({"medical_record_id": {"$in": mrInfo}}), "fkList": []}

    return log1,log2,log3,log4


'''
idList2 = getDistinctValue(“血常规”,“报告单号”, Filters.and(Filters.eq(“性别”, “男”)，Filters.eq(“就诊类型”, “门诊”)，Filters.eq(“送检医师”, “丁楚”)， Filter.lt(“年龄”，38))
avg2 = getAverage(“检验报告”, “检测结果”, Filters.and(Filters.in(“报告单号”, idList2)，Filters.eq(“指标名称”, “白细胞计数”) ) ) 
cnt2 = getConditionCount(“检验报告”, Filters.and(Filters.in(“报告单号”, idList2)，Filters.eq(“指标名称”, “白细胞计数”) ) )

'''
def generateSeq2(a,b,c, seqId):
    f = generateFilter('cc')
    cc = getTable2('cc',f,{"cc_id":1},False,0.01)
    while len(cc) == 0:
        f = generateFilter('cc')
        cc = getTable2('cc', f, {"cc_id": 1}, False, 0.01)
    ccList = list(set(map(lambda x: x["cc_id"], cc)))
    itm = random.choice(jyzb_items)
    jybg = getTable2("jyzb",{"cc_id":{"$in":ccList},"item":itm},{"jyzb_id":1,"res":1},False,0.01)
    jybgList = list(set(map(lambda x: x["jyzb_id"], jybg)))

    log1 = {"serviceName": "getDistinctValue", "relCol": "cc", "relFields": ["cc_id"],
           "relDataList": ccList, "createTime": a, 'reqId': seqId,
           'bson': str(f), "fkList": []}
    log2 = {"serviceName": "getAverage", "relCol": "jyzb", "relFields": ["res"],
           "relDataList": jybgList, "createTime": b, 'reqId': seqId,
           'bson': str({"cc_id":{"$in":ccList},"item":itm}), "fkList": []}
    log3 = {"serviceName": "getConditionCount", "relCol": "jyzb", "relFields": ["res"],
           "relDataList": jybgList, "createTime": c, 'reqId': seqId,
           'bson': str({"cc_id":{"$in":ccList,"item":itm}}),"fkList": []}

    return log1,log2,log3

'''

科室名称 = ['针灸科', '内分泌科', '甲状腺科', '肾内科','泌尿科']
性别=["男","女"]
医生年龄：（25，55）
患者年龄：（1，90）
['甲状腺机能亢进', '甲状腺恶性肿瘤',  '甲状腺炎','unknown']['肾功能衰竭', 'unknown']
就诊类型：['门诊', '住院']
指标名称：['正常','偏高','偏低']
'''

filterDict = {
    "room":['acupuncture', 'endocrinology', 'thyroid', 'nephrology','urology'],
    "sex": ["male","female"],
    "type":['outpatient', 'hospitalized'],
    "res":['normal','high','low']
}

def generateFilter(col):
    colFields = tableInfoDict[col]
    filterList = {}
    if "age" in colFields:
        if random.choice([True,False]):
            if "doctor" in col:
                filterList["age"]={random.choice(["lt","gt"]):random.choice(range(28,53))}
            else:
                filterList["age"]={random.choice(["lt","gt"]):random.choice(range(5,86))}

    for field in colFields:
        f = field
        if f in filterDict:
            flag = random.choice([True,True,True,True,True,False])
            if flag:
                filterList[f] = random.choice(filterDict[f])
    return filterList


def randomAttrs():
    col = random.choice(tableInfoDict.keys())
    colFields = tableInfoDict[col]
    if "age" in colFields or "res" in colFields:
        service = random.choice(serviceList)
        if service in ["getAverage", "getMin", "getMax"]:
            cl = 1
            if "age" in colFields and "res" in colFields:
                fields = [random.choice(["age", "res"])]
            else:
                fields = ["res"] if "res" in colFields else ["age"]
        else:
            if "age" in colFields and "res" in colFields:
                fields = random.sample(["age", "res"], random.choice([1, 2]))
            else:
                fields = ["res"] if "res" in colFields else ["age"]
    else:
        service = random.choice(['projections', 'getCollections', 'collectionStruct', 'getData', 'getConditionCount'])
        fields = random.sample(tableInfoDict[col], random.choice([2, 3]))

    pk = tablePKDict[col]
    if pk not in fields:
        fields += pk,
    filt = generateFilter(col)
    fcond = {}
    for f in fields:
        fcond[f] = 1
    return service, col,fields, filt, fcond

def generateRandom(t, seqId):
    service, col, fields, filt, fcond = randomAttrs()

    info = getTable2(col, filt, fcond, False, 0.01)
    while len(info) == 0:
        service, col, fields, filt, fcond = randomAttrs()
        info = getTable2(col, filt, fcond, False, 0.01)
    if len(info) == 0:
        idList = []
    else:
        idList = list(set(map(lambda x: x[tablePKDict[col]], info)))
    fks = lc.sensitive_check({"relCol":col,"relFields":fields})
    fkInfo = {}
    for fk in fks:
        fkInfo[fk] = list(set(map(lambda x: x[fk], info)))
    log = {"serviceName": service, "relCol": col, "relFields": fields,
           "relDataList": idList, "createTime": t, 'reqId': seqId,
           'bson':str(filt),"fkList":fkInfo}
    return log

def clear():
    r = redis.Redis(host=localhost, port=6379, db=0)
    r.flushall()
    db.drop_collection('LogConf')
    db.drop_collection('LogInfo')



def confInitial():
    dataDB = conn["Data"]
    for table in tableInfoDict.keys():
        data_size = dataDB[table].find().count()
        # print(table, data_size)
        sl = ['projections', 'getData']

        for service in sl:
            logConfTable.insert({'mid':1,'serviceName': service, 'col': table, 'dataSize': data_size,
                                        'safeSG': 0.01, 'exposedDataSG': 0})


def getTime():
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return t



def getRandomTime():
    year = '2017'
    month = 5
    timeList = []
    for day in range(1,32):
        cnt = random.choice(range(700, 1000))
        hour = 8
        min = 0
        for i in range(cnt):
            if min < 59:
                min += 1
            else:
                hour += 1
                min = 1
            if hour >= 24:continue
            sec = random.choice(range(0, 60))
            tm = str(year)+'-' + format(month)+'-' + format(day)+' ' + format(hour)+':' + format(min)+':' + format(sec)
            timeList += tm,
    return timeList


def format(num):
    if len(str(num)) == 2:
        return str(num)
    return '0' + str(num)



def compareDateTime(a,b):
    diff = time.mktime(time.strptime(a, '%Y-%m-%d %H:%M:%S'))-time.mktime(time.strptime(b,'%Y-%m-%d %H:%M:%S'))
    if diff > 0:
        return 1
    elif diff == 0:
        return 0
    else:
        return -1


def generate(n):
    rt = getRandomTime()
    ti = 0
    for i in range(n):
        seqId = "seq" + str(i+1).zfill(6)
        type = random.choice([1]*10+[2]*2+[3]*3)
        if type is 1:
            cnt = random.choice(range(3,7))
            for j in range(cnt):
                log = generateRandom(rt[ti], seqId)
                lc.service_control(log)
                ti += 1
        elif type is 2:
            log1,log2,log3=generateSeq2(rt[ti],rt[ti+1],rt[ti+2],seqId)
            lc.service_control(log1)
            lc.service_control(log2)
            lc.service_control(log3)
            ti += 3
        else:
            log1,log2,log3,log4=generateSeq(rt[i],rt[i+1],rt[i+2],rt[i+3],seqId)
            lc.service_control(log1)
            lc.service_control(log2)
            lc.service_control(log3)
            lc.service_control(log4)
            ti += 4

def oo():
    for log in logConfTable.find():
        sn = log['serviceName']
        col = log['col']
        eds = log['exposedDataSG']
        sf = log['safeSG']
        logConfTable.update({'serviceName': "getData", 'col': col},
                               {'$set': {'exposedDataSG': eds/45.0,'safeSG':0.01}}, False, False)

if __name__== '__main__':
    # lc.clear()
    # lc.conf_data_initial()

    #generate(6000)
    generateRandom("d","1")
    #print(generateRandom("d","1"))
    #print(generateSeq2("","","","1"))

    # log = generateRandom("f","1")
    # print(lc.service_control(log))
    # generateOneLog1028("projections","诊断",["就诊流水号","医生工号"],6001)
    #oo()

    # l = generateFilter("诊断")
    # print(l)
    # print "drug"