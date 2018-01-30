#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import print_function
from __future__ import division

import pymongo
import random
from RandomChar import RandomChar
import fileinput
import RandomNameEn


conn = pymongo.MongoClient('localhost', 27017)
db = conn.DataEn


serviceList = ['getAllValue','getCollections', 'collectionStruct', 'getData', 'getAverage', 'getMax', 'getMin', 'getDataCount']
# 医生基本信息（医生工号、姓名、性别、年龄、住址、科室名称）
# tableInfoDict = {'穿刺': [u'医疗卡号', u'就诊类型', u'性别', u'年龄', u'姓名',u'报告单号',u'报告日期',u'申请人'],
#                  '检验指标':[u'报告单号', u'指标代码', u'参考范围', u'检测结果', u'指标名称',u'异常提示'],
#                  '诊断': [u'科室名称', u'就诊流水号',u'医疗卡号', u'ICD名称', u'诊断时间',u'医生工号'],
#                  '手术': ['医疗卡号','就诊流水号','手术名称','手术日期'],
#                  '患者基本信息': [u'医疗卡号', u'姓名',u'性别',u'年龄'],
#                  '医生基本信息': ['医生工号', '姓名','性别','年龄','住址','科室名称'],
#                  '检查报告头':['检查报告头流水号','医疗卡号','checktype'],
#                  '检验报告':['检验报告流水号''医疗卡号'],
#                  '用药':['用药流水号','药品名称','就诊流水号','用药剂量'],
#                  '检验指标基本信息': [u'指标名称',u'指标代码',u'缩写',u'单位',u'男',u'女',u'新生儿']
#                  }
#                  '诊断': [u'科室名称', u'就诊流水号',u'医疗卡号', u'ICD名称', u'诊断时间',u'医生工号'],

tableInfoDict = {'cc': ['medical_record_id', 'type', 'sex', 'age', 'name','cc_id','time','applicant'],
                 'jyzb':['jyzb_id','cc_id', 'jyzb_code', 'ref_range', 'res', 'item','tip'],
                 'medical_record': ['medical_record_id','room', 'patient_id','ICD', 'time', 'doctor_id'],
                 'patient': ['patient_id', 'name','sex','age','addr'],
                 'doctor': ['doctor_id', 'name','sex','age','addr','room'],
                 'prescription':['prescription_id','drug','medical_record_id','dose'],
                 'jyzb_info': ['jyzb_code','jyzb_name','jyzb_ab','unit','male','female','baby']
                 }


# '患者基本信息': ['医疗卡号', '姓名','性别','年龄','住址'],
def generatePatient():
    for i in range(1000):
        id = str(i).zfill(6)
        sex = random.choice(['male','female'])
        name = RandomNameEn.gen_one_gender_word(sex is 'male')
        db['patient'].insert({'patient_id':id,'name':name,'sex':sex, 'age':random.choice(range(1,90)),'addr':RandomChar().RandomAddress(True) })
    print ('end')

def getPatient():
    patientInfo =[]
    for patient in db['patient'].find():
        patientInfo += {'patient_id': patient['patient_id'],'name':patient['name'],'sex':patient['sex'], 'age':patient['age']},
    return patientInfo

def getTable(table_name,cond,fields):
    tableInfo =[]
    table_struct = tableInfoDict[table_name]
    for info in db[table_name].find(cond,fields):
        detail = {}
        for attr in table_struct:
            detail[attr] = info[attr]
        tableInfo += detail,
    return tableInfo

def getTable2(table_name,cond,fields,flag,perc):
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

    for info in infoList:
        detail = {}
        if fields is None:
            fieldsDict = tableInfoDict[table_name]
        else:
            fieldsDict = fields.keys()
        for attr in fieldsDict:
            detail[attr] = info[attr]
        tableInfo += detail,
    if len(tableInfo) == 0:
        return tableInfo
    tableInfo_r = random.sample(tableInfo,max(1,int(len(tableInfo)*perc)))
    return tableInfo_r


# '诊断': [u'科室名称', u'就诊流水号',u'医疗卡号', u'ICD名称', u'诊断时间',u'医生工号'],
# 'medical_record': ['medical_record_id','room', 'patient_id','ICD', 'time', 'doctor_id'],
def generateDiagnose():
    doctorDict = {}
    #roomList = ['针灸科',      '内分泌科',       '甲状腺科', '肾内科',    '泌尿科']
    roomList = ['acupuncture', 'endocrinology', 'thyroid', 'nephrology','urology']

    for room in roomList:
        doctorDict[room] = getTable("doctor",{"room":room},{"_id":0})
    #'医生基本信息': ['医生工号', '姓名', '性别', '年龄', '住址', '科室名称'],
    # dList1 = ['甲状腺机能亢进', '甲状腺恶性肿瘤',  '甲状腺炎','unknown']
    dList1 = ['hyperthyrea', 'AMTT', 'thyroiditis','unknown']
    for i in range(5000):
        room = random.choice(roomList)
        id = str(i).zfill(8)
        pid = str(random.choice(range(1000))).zfill(6)
        icdName = 'unknown'
        if room is 'thyroid' or room is 'endocrinology':
            icdName = random.choice(dList1)
        if room is 'nephrology' or room is 'urology':
            # icdName = random.choice(['肾功能衰竭', 'unknown'])
            icdName = random.choice(['kidney', 'unknown'])
        #print (icdName)

        docId = random.choice(doctorDict[room])
        db['medical_record'].insert({'room':room,'medical_record_id':id,'patient_id':pid,
                         'ICD':icdName, 'time': RandomChar().RandomTime(),
                         'doctor_id':docId['doctor_id']})
    print ('end')

#医生基本信息（医生工号、姓名、性别、年龄、住址、科室名称）
def generateDoctor():
    # roomList = ['针灸科',      '内分泌科',       '甲状腺科', '肾内科',    '泌尿科']
    roomList = ['acupuncture', 'endocrinology', 'thyroid', 'nephrology', 'urology']
    for i in range(100):
        id = str(i).zfill(6)
        sex = random.choice(['male','female'])
        name = RandomNameEn.gen_one_gender_word(sex is 'male')
        db['doctor'].insert({'doctor_id':id,'name':name,'sex':sex, 'age':random.choice(range(25,55)),
                             'addr':RandomChar().RandomAddress(True), 'room':random.choice(roomList)})
    print ('end')

# 检验指标基本信息 [指标名称，指标代码，缩写，单位，男，女，新生儿]
#'jyzb_info': ['jyzb_code', 'jyzb_name', 'jyzb_ab', 'unit', 'male', 'female', 'baby']
def generateJYZBJBXX():
    i = 0
    name,id, scode, unit, man, wm, chd = '','','','','','',''
    for line in fileinput.input('../file/BloodRoutine'):
        ll = line.strip()
        if i % 2 == 0:
            if not not name:
                db["jyzb_info"].insert({'jyzb_name':scode, 'jyzb_code':id, 'jyzb_ab':scode,'unit':unit,'male':man,'female':wm,'baby':chd})

            name = ll.split('(')[0]
            id = str(int(i/2)+1).zfill(4)
            scode = ll.split('(')[1].split(')')[0]
            unit = ll.split('：')[1].replace(')', '').replace('；', '').replace('。', '')
        else:
            infoList = map(lambda info : info.split('：')[1].strip().replace("；","").replace("。","").replace(";",""), ll.split('，'))
            man, wm, chd = infoList
        i += 1

    if not not name:
        db["jyzb_info"].insert({'jyzb_name': scode, 'jyzb_code': id, 'jyzb_ab': scode, 'unit': unit, 'male': man, 'female': wm, 'baby': chd})



# '穿刺': ['医疗卡号', '就诊类型', '性别', '年龄', '姓名','报告单号','报告日期','申请人']
# '诊断': [u'科室名称', u'就诊流水号',u'医疗卡号', u'ICD名称', u'诊断时间',u'医生工号'],
# 医生基本信息（医生工号、姓名、性别、年龄、住址、科室名称）
# 'medical_record': ['medical_record_id','room', 'patient_id','ICD', 'time', 'doctor_id'],
# 'doctor': ['doctor_id', 'name','sex','age','addr','room'],
# 'cc': ['medical_record_id', 'type', 'sex', 'age', 'name','cc_id','time','applicant'],
def generateCC():
    #patientList = getPatient()
    # 'AMTT', 'thyroiditis'
    diagnoseList = getTable("medical_record",{"ICD":{'$in':["AMTT",'thyroiditis']}},{"_id":0})
    i = 1
    doctorList = getTable("doctor",{},{"_id":0})
    for diagnose in diagnoseList:
        p = db["patient"].find_one({"patient_id":diagnose["patient_id"]})
        d = db["doctor"].find_one({"doctor_id":diagnose["doctor_id"]})
        if random.choice([True,True,True, False]):
            db['cc'].insert({'patient_id': p['patient_id'], 'type': random.choice(['outpatient', 'hospitalized']),
                             'sex': p['sex'], 'age': p['age'],'medical_record_id':diagnose['medical_record_id'],
                             'name': p['name'], 'cc_id': "ccbg" + (str(i)).zfill(4),
                             'time': RandomChar().RandomTime(), 'applicant': d['name']})
            i += 1
    print('end')

# '检验指标':['报告单号', '穿刺单号', '指标代码',   '参考范围',   '检测结果', '指标名称',  '异常提示'],
# 'jyzb':['jyzb_id',   'cc_id',    'jyzb_code', 'ref_range', 'res',    'item',    'tip'],
def generateJYBG():
    yctsList = ['normal','high','low','normal','normal','normal','high','low','high','low']
    ccList = getTable('cc',{},{"_id":0})
    jyzb_info = getTable('jyzb_info',{}, {"_id":0})
    id = 1
    for cc in ccList:
        for jyzb in jyzb_info:
            if cc['age'] <= 1:
                ccfw = jyzb['baby']
            else:
                ccfw = jyzb['male'] if cc['sex'] == 'male' else jyzb['female']
            #ccfw = ccfw.encode('utf8')
            ycts = random.choice(yctsList)
            if ycts == 'normal':
                yctsTag = 0
            elif ycts == 'high':
                yctsTag = 1
            else:
                yctsTag = -1

            unit = jyzb['unit']
            jcjg = RandomChar().RandomBXB(ccfw, yctsTag, unit)
            db['jyzb'].insert({'jyzb_id':'jyzb'+str(id).zfill(4),'cc_id':cc['cc_id'],'jyzb_code':jyzb['jyzb_code'],
                                   'ref_range':ccfw,'res':jcjg,'item':jyzb['jyzb_name'],'tip':ycts})
            id += 1


#'用药':['用药流水号','药品名称','就诊流水号','用药剂量'],
#'prescription':['prescription_id','drug','medical_record_id','dose']
def generatePrescription():
    dList1 = ['hyperthyrea', 'AMTT', 'thyroiditis','kidney','unknown']
    #dList1 = ['甲状腺机能亢进', '甲状腺恶性肿瘤',  '甲状腺炎','肾功能衰竭','unknown']
    drugDict={}
    for icd in dList1:
        icdDrug = []
        for i in range(random.choice(range(30, 50))):
            icdDrug += (icd+'_drug_'+str(i).zfill(2)),
        drugDict[icd] = icdDrug
    diagnoseList = getTable("medical_record",{},{"_id": 0})
    prescId = 1
    for d in diagnoseList:
        #'诊断': [u'科室名称', u'就诊流水号', u'医疗卡号', u'ICD名称', u'诊断时间', u'医生工号'],
        # print d.keys()
        # print d[unicode('ICD名称')]
        drugList = random.sample(drugDict[d["ICD"]],random.choice([2,3,4,5]))
        for drug in drugList:
            id = str(prescId).zfill(10)
            db['prescription'].insert({'prescription_id':id,'drug':drug,'medical_record_id':d['medical_record_id'], 'dose':random.choice([1,2,3,4,5])})
            prescId += 1

    print ('end')





if __name__== '__main__':
    # getTableToDict('穿刺','报告单号')
    #generateJYZBJBXX()
    # OK:generateDoctor()
    # OK:generatePatient()
    # OK:generateDiagnose()
    # OK:generateCC()
    generatePrescription()
    #generateDrug()
    #generateJYBG()

    # a = getTable("医生基本信息",{'科室名称':'甲状腺科'},{})
    #getCCJYBG()
    #db['患者基本信息'].insert({'医疗卡号': 'd'})
    print ('main')
