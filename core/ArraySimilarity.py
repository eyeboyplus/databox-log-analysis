#!/usr/bin/python
# -*- coding: UTF-8 -*-
import random,time,math

# 数组预判断，数组长度相差较大时，不进入相似度计算
# 相似度定义为 重合区域大小/两数组的最大size
# 返回相似度是否超过阈值，超过阈值时的相似度大小 以及 除去重合区域的两数组
# 数据量：3000-1s内。30000-20+s。40000-
def arrSim(arrA, arrB, sim):
    tmpa = arrA[:]
    tmpb = arrB[:]
    maxS = max(len(arrA), len(arrB))

    minNeq = maxS * (1. - sim)
    eqCnt = 0
    neqCnt = 0
    for i in arrA:
        if neqCnt < minNeq:
            if i in arrB:
                eqCnt += 1
                tmpa.remove(i)
                tmpb.remove(i)
            else:
                neqCnt += 1

    cursim = eqCnt * 1. / maxS
    return cursim > sim

# 数组预判断，数组长度相差较大时，不进入相似度计算
# 相似度定义为 重合区域大小/两数组的最大size
# 返回重复元素大于重复最小值sim，
# 数据量：3000-1s内。30000-20+s。40000-
def dataSim(arrA,arrB, sim, lenA):
    lenB = len(arrB)
    # 不是相似序列
    if abs(lenA - lenB) > sim:
        return False

    if lenA > lenB:
        maxArr, minArr = arrA, arrB
    else:
        maxArr, minArr = arrB, arrA

    # 如果超过有minNeq个不重复，必定不相似
    #
    minNeq = len(minArr) - sim
    eqCnt = 0
    neqCnt = 0
    for i in minArr:
        if eqCnt >= sim:
            return True
        elif neqCnt > minNeq:
            return False
        else:
            if i in maxArr:
                eqCnt += 1
            else:
                neqCnt += 1

    return eqCnt >= sim


if __name__ == "__main__":
    a = random.sample(range(50000),40000)
    b = random.sample(range(50000),40000)
    a = map(lambda x:'docid'+str(x),a)
    b = map(lambda x:'docid'+str(x),b)
    print('generated array')
    st = time.time()
    print(dataSim(a,b,0.1))
    et = time.time()
    print(et - st)
