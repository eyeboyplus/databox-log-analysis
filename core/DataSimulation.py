#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division

import pymongo
import random

from RandomChar import RandomChar
import fileinput

conn = pymongo.MongoClient("localhost", 27017)
db = conn.Data


serviceList = ["getAllValue","getCollections", "collectionStruct", "getData", "getAverage", "getMax", "getMin", "getDataCount"]
# 医生基本信息（医生工号、姓名、性别、年龄、住址、科室名称）
tableInfoDict = {"穿刺": [u"医疗卡号", u"就诊类型", u"性别", u"年龄", u"姓名",u"报告单号",u"报告日期",u"申请人"],
                 "检验指标":[u"报告单号", u"指标代码", u"参考范围", u"检测结果", u"指标名称",u"异常提示"],
                 "诊断": ["科室名称", "就诊流水号","医疗卡号", "ICD名称", "诊断时间","医生工号"],
                 "手术": ["医疗卡号","就诊流水号","手术名称","手术日期"],
                 "患者基本信息": [u"医疗卡号", u"姓名",u"性别",u"年龄"],
                 "医生基本信息": [u"医生工号", u"姓名",u"性别",u"年龄",u"住址",u"科室名称"],
                 "检查报告头":["检查报告头流水号","医疗卡号","checktype"],
                 "检验报告":["检验报告流水号""医疗卡号"],
                 "用药":["用药流水号","药品名称","就诊流水号","用药剂量"],
                 "检验指标基本信息": [u"指标名称",u"指标代码",u"缩写",u"单位",u"男",u"女",u"新生儿"]
                 }


# "患者基本信息": ["医疗卡号", "姓名","性别","年龄","住址"],
def generatePatient():
    for i in range(1000):
        name = RandomChar().RandomName()
        id = str(i).zfill(6)
        sex = random.choice(["男","女"])
        db["患者基本信息"].insert({"医疗卡号":id,"姓名":name,"性别":sex, "年龄":random.choice(range(1,90)),"住址":RandomChar().RandomAddress() })
    print "end"

def getPatient():
    patientInfo =[]
    for patient in db["患者基本信息"].find():
        patientInfo += {"医疗卡号": patient[u"医疗卡号"],"姓名":patient[u"姓名"],"性别":patient[u"性别"], "年龄":patient[u"年龄"]},
    return patientInfo

def getTable(table_name,cond,fields,flag,perc):
    tableInfo =[]
    if flag:
        if fields is None:
            infoList = db[table_name].find(cond).distinct("str")
        else:
            infoList = db[table_name].find(cond, fields).distinct("str")
    else:
        if fields is None:
            infoList = db[table_name].find(cond)
        else:
            infoList = db[table_name].find(cond, fields)

    # a=db["患者基本信息"].find().count()
    # tableInfo = db[table_name].find(cond, fields).count()
    for info in infoList:
        detail = {}
        if fields is None:
            fieldsDict = tableInfoDict[table_name]
        else:
            fieldsDict = fields.keys()
        for attr in fieldsDict:
            detail[attr] = unicode(info[unicode(attr)]).encode("utf8")
        tableInfo += detail,
    if len(tableInfo) == 0:
        return tableInfo
    tableInfo_r = random.sample(tableInfo,max(1,int(len(tableInfo)*perc)))
    return tableInfo_r


# "诊断": [u"科室名称", u"就诊流水号",u"医疗卡号", u"ICD名称", u"诊断时间",u"医生工号"],
def generateDiagnose():
    doctorDict = {}
    roomList = ["针灸科", "内分泌科", "甲状腺科", "肾内科","泌尿科"]
    for room in roomList:
        doctorDict[room] = getTable("医生基本信息",{"科室名称":room})
    #"医生基本信息": ["医生工号", "姓名", "性别", "年龄", "住址", "科室名称"],
    dList1 = ["甲状腺机能亢进", "甲状腺恶性肿瘤",  "甲状腺炎","unknown"]
    for i in range(5000):
        room = random.choice(roomList)
        id = str(i).zfill(8)
        pid = str(random.choice(range(1000))).zfill(6)
        icdName = "unknown"
        if room is "甲状腺科" or  room is "内分泌科":
            icdName = random.choice(dList1)
        if room is "肾内科" or room is "泌尿科":
            icdName = random.choice(["肾功能衰竭", "unknown"])
        print icdName

        docId = random.choice(doctorDict[room])
        db["诊断"].insert({"科室名称":room,"就诊流水号":id,"医疗卡号":pid,
                         "ICD名称":icdName, "诊断时间": RandomChar().RandomTime(),
                         "医生工号":docId["医生工号"]})
    print "end"

#医生基本信息（医生工号、姓名、性别、年龄、住址、科室名称）
def generateDoctor():
    roomList = ["针灸科", "内分泌科", "甲状腺科", "肾内科","泌尿科"]
    for i in range(100):
        name = RandomChar().RandomName()
        id = str(i).zfill(6)
        sex = random.choice(["男","女"])
        db["医生基本信息"].insert({"医生工号":id,"姓名":name,"性别":sex, "年龄":random.choice(range(25,55)),
                             "住址":RandomChar().RandomAddress(), "科室名称":random.choice(roomList)})
    print "end"

# 检验指标基本信息 [指标名称，指标代码，缩写，单位，男，女，新生儿]
def generateJYZBJBXX():
    i = 0
    name,id, scode, unit, man, wm, chd = "","","","","","",""
    for line in fileinput.input("../file/BloodRoutine"):
        ll = line.strip()
        if i % 2 == 0:
            if not not name:
                db["检验指标基本信息"].insert({"指标名称":name, "指标代码":id, "缩写":scode,"单位":unit,"男":man,"女":wm,"新生儿":chd})
            #print i

            name = ll.split("(")[0]
            id = str(int(i/2)+1).zfill(4)
            scode = ll.split("(")[1].split(")")[0]
            unit = ll.split("：")[1].replace(")", "").replace("；", "").replace("。", "")
        else:
            #print ll.split("，")
            infoList = map(lambda info : info.split("：")[1].strip(), ll.split("，"))
            man, wm, chd = infoList
        i += 1

    if not not name:
        db["检验指标基本信息"].insert({"指标名称": name, "指标代码": id, "缩写": scode, "单位": unit, "男": man, "女": wm, "新生儿": chd})



#"穿刺": ["医疗卡号", "就诊类型", "性别", "年龄", "姓名","报告单号","报告日期","申请人"]
# "诊断": [u"科室名称", u"就诊流水号",u"医疗卡号", u"ICD名称", u"诊断时间",u"医生工号"],
#医生基本信息（医生工号、姓名、性别、年龄、住址、科室名称）
def generateCC():
    #patientList = getPatient()
    diagnoseList = getTable("诊断",{"ICD名称":{"$in":["甲状腺恶性肿瘤","甲状腺炎"]}})
    i = 1
    doctorList = getTable("医生基本信息",{})
    for diagnose in diagnoseList:
        p = db["患者基本信息"].find_one({"医疗卡号":diagnose[u"医疗卡号"]})
        d = db["医生基本信息"].find_one({"医生工号":diagnose[u"医生工号"]})
        if random.choice([True,True,True, False]):
            db["穿刺"].insert({"医疗卡号": p[u"医疗卡号"], "就诊类型": random.choice(["门诊", "住院"]),
                             "性别": p[u"性别"], "年龄": p[u"年龄"],
                             "姓名": p[u"姓名"], "报告单号": "ccbg" + (str(i)).zfill(4),
                             "报告日期": RandomChar().RandomTime(), "申请人": d[u"姓名"]})
            i += 1
    # for p in patientList:
    #     if random.choice([True, False]):
    #         db["穿刺"].insert({"医疗卡号":p["医疗卡号"], "就诊类型":random.choice(["门诊","住院"]),
    #                          "性别":p["性别"], "年龄":p["年龄"],
    #                          "姓名":p["姓名"],"报告单号": "ccbg" +(str(i)).zfill(4),
    #                          "报告日期":RandomChar().RandomTime(),"申请人":random.choice(doctorList)})
    #         i += 1
    print "end"

# "检验指标":["报告单号", "指标代码", "参考范围", "检测结果", "指标名称","异常提示"],
def generateJYBG():
    ccList = getTable("穿刺",{},None,False)
    jyzb = db["检验指标基本信息"].find_one({"指标名称":"白细胞计数"})
    yctsList = ["正常","偏高","偏低","正常","正常","正常","偏高","偏低","偏高","偏低"]
    for cc in ccList:
        if cc[unicode("年龄")] <= 1:
            ccfw = jyzb[u"新生儿"]
        else:
            ccfw = jyzb[u"男"] if cc[u"性别"] == "男" else jyzb[u"女"]
        ccfw = ccfw.encode("utf8")
        ycts = random.choice(yctsList)
        if ycts == "正常":
            yctsTag = 0
        elif ycts == "偏高":
            yctsTag = 1
        else:
            yctsTag = -1
        jcjg = RandomChar().RandomBXB(ccfw, yctsTag)
        db["检验指标"].insert({"报告单号":cc[u"报告单号"],"指标代码":jyzb[u"指标代码"],
                               "参考范围":ccfw,"检测结果":jcjg,"指标名称":jyzb[u"指标名称"],"异常提示":ycts})


#"用药":["用药流水号","药品名称","就诊流水号","用药剂量"],
def generatePrescription():
    dList1 = ["甲状腺机能亢进", "甲状腺恶性肿瘤",  "甲状腺炎","肾功能衰竭","unknown"]
    drugDict={}
    for icd in dList1:
        icdDrug = []
        for i in range(random.choice(range(30,50))):
            icdDrug += (icd+"药品"+str(i).zfill(2)),
        drugDict[unicode(icd).encode("utf8")] = icdDrug
    diagnoseList = getTable("诊断",{})
    prescId = 1
    for d in diagnoseList:
        #"诊断": [u"科室名称", u"就诊流水号", u"医疗卡号", u"ICD名称", u"诊断时间", u"医生工号"],
        # print d.keys()
        # print d[unicode("ICD名称")]
        drugList = random.sample(drugDict[d[unicode("ICD名称")]],random.choice([2,3,4,5]))
        for drug in drugList:
            id = str(prescId).zfill(10)
            db["用药"].insert({"用药流水号":id,"药品名称":drug,"就诊流水号":d[u"就诊流水号"], "用药剂量":random.choice([1,2,3,4,5])})
            prescId += 1

    print "end"


def getTableToDict(table_name, field):
    tableInfo = {}
    table_struct = tableInfoDict[table_name]
    for info in db[table_name].find():
        detail = {}
        for attr in table_struct:
            detail[attr.encode("utf8")] = info[unicode(attr)]
        tableInfo[info[unicode(field)].encode("utf8")] = detail
    return tableInfo

def getCCJYBG():
    ccDict = getTableToDict("穿刺","报告单号")
    jyzbDict = getTableToDict("检验指标","报告单号")
    outFile = open("test.csv","w")
    title = [u"医疗卡号", u"报告单号", u"就诊类型", u"性别", u"年龄", u"姓名", u"报告日期", u"申请人", u"指标代码", u"参考范围", u"检测结果", u"指标名称", u"异常提示"]
    outFile.write("|".join(title)+"\n")
    res = {}
    for cck in ccDict.keys():
        res[cck] = ccDict[cck].copy()
        res[cck].update(jyzbDict[cck])
        line = ""
        for t in title:
            line += str(res[cck][t.encode("utf8")]) +"|"
        outFile.write(line+"\n")
    outFile.close()
        #db["测试"].insert(res[cck])
    return res


if __name__== "__main__":
    # getTableToDict("穿刺","报告单号")
    # ok: generateDoctor()
    # ok: generatePatient()
    # ok: generateDiagnose()
    # ok: generatePrescription()
    # ok: generateJYZBJBXX()
    #generateCC()
    # generateDrug()
    # ok: generateJYBG()
    p = getPatient()
    prescInfo = getTable("诊断",{"ICD名称":{"$in":["甲状腺机能亢进", "甲状腺恶性肿瘤",  "甲状腺炎"]}},{"就诊流水号":1,"医疗卡号":1,"医生工号":1},False,0.2)
    #getCCJYBG()
    #db["患者基本信息"].insert({"医疗卡号": "d"})
    print "main"
