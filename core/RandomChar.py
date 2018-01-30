#!/usr/bin/python
# -*- coding: UTF-8 -*-
import random,string

lastName = ['黄', '冯', '苏', '沈', '丁', '魏', '薛', '方', '桂', '唐', '汤', '路', '卢', '吕', '蔡', '曹',
                    '赵', '钱', '孙', '李', '周', '吴', '郑', '王', '张', '刘', '陈', '胡', '郭', '和', '何', '谢',
                    '于', '宋', '马', '彭', '韩']

class RandomChar():
    """用于随机生成汉字"""

    @staticmethod
    def Unicode():
        val = random.randint(0x4E00, 0x9FBF)
        return unichr(val).encode('utf8')

    @staticmethod
    def GB2312():
        head = random.randint(0xB0, 0xCF)
        body = random.randint(0xA, 0xF)
        tail = random.randint(0, 0xF)
        val = (head << 8) | (body << 4) | tail
        str = "%x" % val
        return str.decode('hex').decode('gb2312').encode('utf8')

    @staticmethod
    def RandomName():
        try:
            name = random.choice(lastName)
            for i in range(random.choice([1,2])):
                name += RandomChar().GB2312()
            return name
        except:
            return RandomChar().RandomName()

    @staticmethod
    def RandomEnName():
        try:
            name = random.choice(lastName)
            for i in range(random.choice([1,2])):
                name += RandomChar().GB2312()
            return name
        except:
            return RandomChar().RandomName()


    @staticmethod
    def RandomAddress(En=False):
        if not En:
            district = ['浦东新区','杨浦区','黄浦区','徐汇区','长宁区','静安区','普陀区','闵行区']
            return "上海" + random.choice(district)
        else:
            district = ['Pudong','Yangpu','Huangpu','Xuhui','Changning','Jingan','Putuo','Minhang']
            return "Shanghai" + random.choice(district)


    @staticmethod
    def RandomTime():
        year = '2016'

        month = random.choice(range(1, 13))

        if month in [1, 3, 5, 7, 8, 10, 12]:
            day = random.choice(range(1, 32))
        elif month == 2:
            day = random.choice(range(1, 30))
        else:
            day = random.choice(range(1, 31))
        hour = random.choice(range(0, 24))
        min = random.choice(range(0, 60))
        sec = random.choice(range(0, 60))

        time = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2) + ' ' \
               + str(hour).zfill(2) + ':' + str(min).zfill(2) +':'+ str(sec).zfill(2)
        return time

    @staticmethod
    def RandomBXB(bxbRange, tag, unit="%"):
        minR, maxR = map(float, bxbRange.replace(unit.strip(), "").replace("*","").strip().split("-"))
        if tag == 0:
            return random.choice(range(int(minR * 100), int(maxR * 100) + 1)) / 100.
        elif tag < 0:
            return random.choice(range(1, int(minR * 100))) / 100.
        else:
            return random.choice(range(int(maxR * 100) + 1, int(2 * maxR * 100))) / 100.
        # try:
        #     minR, maxR = map(float, bxbRange.replace(";","").replace("。","").replace(unit.strip(),"").replace("*","").strip().split("-"))
        #     if tag == 0:
        #         return random.choice(range(int(minR*100),int(maxR*100)+1))/100.
        #     elif tag < 0:
        #         return random.choice(range(1,int(minR*100)))/100.
        #     else:
        #         return random.choice(range(int(maxR*100) + 1,int(2*maxR*100)))/100.
        # except:
        #
        #     tmp = bxbRange.replace(";","").replace(unit.strip(),"").replace("*","")
        #     print(tmp)
        #     print (bxbRange,tag,unit)

if __name__== '__main__':
    rc = RandomChar()
    salt = ''.join(random.sample(string.digits, 8))

    #print rc.RandomBXB('4-10*10^9/L',1)