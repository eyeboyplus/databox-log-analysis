#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import pymongo,redis
import sys
import random
import json
import time
from DataSimulation import *
from LogCase import *
reload(sys)
sys.setdefaultencoding('utf8')

tableNameSuffix = 'LogStats'

serviceList = ['projections','getCollections', 'collectionStruct', 'getData', 'getAverage', 'getMax', 'getMin', 'getConditionCount',"getDistinctValue"]
bsonParamList = ['eq', 'ne', 'in', 'nin', 'and', 'or', 'nor', 'not', 'exists', 'gt', 'gte', 'lt', 'lte']
colNameList = ['Diabetes', 'Hypertension', 'Uremia', 'HeartDisease']
# 糖尿病，高血压，尿毒症，心脏病
fieldNameList = ['pName', 'pAge', 'pGender', 'dDate', 'isCured']
# ['Diabetes.pName', 'Diabetes.pAge', 'Diabetes.pGender', 'Diabetes.dDate', 'Diabetes.isCured',
#            'Hypertension.pName', 'Hypertension.pAge', 'Hypertension.pGender', 'Hypertension.dDate', 'Hypertension.isCured',
#            'Uremia.pName', 'Uremia.pAge', 'Uremia.pGender', 'Uremia.dDate', 'Uremia.isCured',
#            'HeartDisease.pName', 'HeartDisease.pAge', 'HeartDisease.pGender', 'HeartDisease.dDate', 'HeartDisease.isCured' ]
# 病人姓名，年龄，性别，确诊日期，是否治愈，

# 1千万数据
dataSize = 10000000
selectPec = [0.00005, 0.00010, 0.00015, 0.00020, 0.00025, 0.00030]
# 0.005%*10000000 = 500~3000



conn = pymongo.MongoClient('localhost', 27017)
db = conn.Logs2017
logTable = db.LogInfo
statsTable = db.LogStats
statsTestTable = db.LogStatsTest
logConfTable = db.LogConf
# collectionList = db.collection_names()

# colStatList = {}
# for collection in colNameList:
#     print(collection, db[collection].find().count())

#print(logTable.find().count())

'''

科室名称 = ['针灸科', '内分泌科', '甲状腺科', '肾内科','泌尿科']
性别=["男","女"]
医生年龄：（25，55）
患者年龄：（1，90）
['甲状腺机能亢进', '甲状腺恶性肿瘤',  '甲状腺炎','unknown']['肾功能衰竭', 'unknown']
就诊类型：['门诊', '住院']
指标名称：['正常','偏高','偏低']
'''
tableInfoDict = {'穿刺': [u'医疗卡号', u'就诊类型', u'性别', u'年龄', u'姓名',u'报告单号',u'报告日期',u'申请人'],
                 '检验指标':[u'报告单号', u'指标代码', u'参考范围', u'检测结果', u'指标名称',u'异常提示'],
                 '诊断': [u'科室名称', u'就诊流水号',u'医疗卡号', u'ICD名称', u'诊断时间',u'医生工号'],
                 '患者基本信息': [u'医疗卡号', u'姓名',u'性别',u'年龄',u'住址'],
                 '医生基本信息': ['医生工号', '姓名','性别','年龄','住址','科室名称'],
                 '用药':['用药流水号','药品名称','就诊流水号','用药剂量']
                 }
tablePKDict = {
                 '穿刺': "报告单号",
                 '检验指标':"报告单号",
                 '诊断': "就诊流水号",
                 '患者基本信息':"医疗卡号",
                 '医生基本信息': "医生工号",
                 '用药':"用药流水号",
}

redis_name_dict = get_db_conf('../file/conf')
# def generate1026():
#     prescInfo = getTable("诊断",{"ICD名称":{"$in":["甲状腺机能亢进", "甲状腺恶性肿瘤",  "甲状腺炎"]}},{"就诊流水号":1,"医疗卡号":1,"医生工号":1},False)
#     # patientInfo = getTable("诊断",{"医疗卡号":1},True)
#     patientInfo = list(set(map(lambda x:x["医生工号"], prescInfo)))
#     # mrInfo = getTable("诊断",{"就诊流水号":1},True)
#     mrInfo = list(set(map(lambda x:x["就诊流水号"], prescInfo)))
#     drugInfo = getTable("用药",{"就诊流水号":{"$in": mrInfo }},{"就诊流水号":1,"药品名称":1},False)
#     a = drugInfo

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
    pk = unicode(tablePKDict[relCol])
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
    prescInfo = getTable("诊断", {"ICD名称": {"$in": ["甲状腺机能亢进", "甲状腺恶性肿瘤", "甲状腺炎"]}}, {"就诊流水号": 1, "医疗卡号": 1, "医生工号": 1},False, 0.01)
    patientInfo = list(set(map(lambda x: x["医生工号"], prescInfo)))
    mrInfo = list(set(map(lambda x: x["就诊流水号"], prescInfo)))
    drugInfo = getTable("用药", {"就诊流水号": {"$in": mrInfo}}, {"用药流水号":1 ,"就诊流水号": 1, "药品名称": 1}, False,0.01)
    drugList = list(set(map(lambda x:x["用药流水号"],drugInfo)))
    generateOneLog1026("projections", "诊断", ["就诊流水号","医疗卡号","医生工号"], mrInfo, a, seqId)
    generateOneLog1026("getDistinctValue", "诊断", ["医疗卡号"], mrInfo, b, seqId)
    generateOneLog1026("getDistinctValue", "诊断", ["就诊流水号"], mrInfo, c, seqId)
    generateOneLog1026("projections", "用药", ["就诊流水号","药品名称"], drugList, d, seqId)
    # logTable.insert(log1)
    # logTable.insert(log2)
    # logTable.insert(log3)
    # logTable.insert(log4)

'''
idList2 = getDistinctValue(“血常规”,“报告单号”, Filters.and(Filters.eq(“性别”, “男”)，Filters.eq(“就诊类型”, “门诊”)，Filters.eq(“送检医师”, “丁楚”)， Filter.lt(“年龄”，38))
avg2 = getAverage(“检验报告”, “检测结果”, Filters.and(Filters.in(“报告单号”, idList2)，Filters.eq(“指标名称”, “白细胞计数”) ) ) 
cnt2 = getConditionCount(“检验报告”, Filters.and(Filters.in(“报告单号”, idList2)，Filters.eq(“指标名称”, “白细胞计数”) ) )

'''
def generateSeq2(a,b,c, seqId):
    cc = getTable('穿刺',{"性别":"男","就诊类型":"门诊","年龄":{"$lt":50}},{"报告单号":1},False,0.01)
    ccList = list(set(map(lambda x: x["报告单号"], cc)))
    jybg = getTable("检验指标",{"报告单号":{"$in":ccList},"指标名称":"白细胞计数"},{"报告单号":1,"检测结果":1},False,0.01)
    jybgList = list(set(map(lambda x: x["报告单号"], jybg)))

    generateOneLog1026("getDistinctValue","穿刺",["报告单号"],ccList,a, seqId)
    generateOneLog1026("getAverage","检验指标",["检测结果"],jybgList, b, seqId)
    generateOneLog1026("getConditionCount","检验指标",["检测结果"],jybgList, c, seqId)
    # logTable.insert(log1)
    # logTable.insert(log2)
    # logTable.insert(log3)

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
    "科室名称":['针灸科', '内分泌科', '甲状腺科', '肾内科','泌尿科'],
    "性别":["男","女"],
    "就诊类型":['门诊', '住院'],
    "指标名称":['正常','偏高','偏低'],
}
def generateFilter(col):
    colFields = tableInfoDict[col]
    filterList = {}
    if u"年龄" in colFields:
        if random.choice([True,False]):
            if "医生" in col:
                filterList["年龄"]={random.choice(["lt","gt"]):random.choice(range(28,53))}
            else:
                filterList["年龄"]={random.choice(["lt","gt"]):random.choice(range(5,86))}

    for field in colFields:
        f = field.encode("utf8")
        if f in filterDict:
            flag = random.choice([True,True,True,True,True,False])
            if flag:
                filterList[f] = random.choice(filterDict[f])
    return filterList


def generateRandom(a,seqId):
    col = random.choice(tableInfoDict.keys())
    colFields = tableInfoDict[col]
    if u"年龄" in colFields or u"检测结果" in colFields:
        service = random.choice(serviceList)
        if service in ["getAverage", "getMin", "getMax"]:
            cl = 1
            if u"年龄" in colFields and u"检测结果" in colFields:
                fields = [random.choice([u"年龄",u"检测结果"])]
            else:
                fields = [u"检测结果"] if  u"检测结果" in colFields else [u"年龄"]
        else:
            if u"年龄" in colFields and u"检测结果" in colFields:
                fields = random.sample([u"年龄",u"检测结果"],random.choice([1,2]))
            else:
                fields = [u"检测结果"] if  u"检测结果" in colFields else [u"年龄"]
    else:
        service = random.choice(['projections','getCollections', 'collectionStruct', 'getData', 'getConditionCount'])
        fields = random.sample(tableInfoDict[col], random.choice([2,3]))

    pk = unicode(tablePKDict[col])
    if pk not in fields:
        fields += pk,
    filt = generateFilter(col)
    fcond = {}
    for f in fields:
        fcond[f] = 1

    info = getTable(col, filt, fcond, False, 0.01)

    if len(info) == 0:
        idList = []
    else:
        try:
            idList = list(set(map(lambda x: x[tablePKDict[col]], info)))
        except:
            idList = list(set(map(lambda x: x[unicode(tablePKDict[col])], info)))
    generateOneLog1026(service,col,fields,idList,a, seqId)

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
                                        'safeSG': 0.45, 'exposedDataSG': 0})


def getTime():
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return t

# 随机生成一条log日志
def generateOneLog():
    serviceName = random.choice(serviceList)
    # bsonParam =
    relCol = random.sample(colNameList, random.choice([1, 2]))
    relFileds = getRelFields(relCol)
    relDataList = getRelDataList(relCol)
    createdTime = getRandomTime()
    logDetail = {'serviceName': serviceName, 'relCol': relCol, 'relFields': relFileds,
                 'relDataList': relDataList, 'createdTime': createdTime}
    return logDetail


def getRelFields(relCol):
    resList = []
    for c in relCol:
        fList = random.sample(fieldNameList, random.choice([2, 3]))
        for f in fList:
            resList.append(c + "." + f)
    return resList


def getRelDataList(relCol):
    resList = {}
    for c in relCol:
        dList = sorted(random.sample(range(1, dataSize), int(random.choice(selectPec) * dataSize)))
        resList[c] = map(str, dList)
    return resList


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


#  随机生成count条log记录
def generateLogs(count):
    for i in range(0, count):
        logTable.insert(generateOneLog())


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
                generateRandom(rt[ti], seqId)
                ti += 1
        elif type is 2:
            generateSeq2(rt[ti],rt[ti+1],rt[ti+2],seqId)
            ti += 3
        else:
            generateSeq(rt[i],rt[i+1],rt[i+2],rt[i+3],seqId)
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
    # clear()
    # confInitial()
    #
    # generate(6000)
    # generateOneLog1028("projections","诊断",["就诊流水号","医生工号"],6001)
    oo()

    # l = generateFilter("诊断")
    # print(l)
    # print "drug"