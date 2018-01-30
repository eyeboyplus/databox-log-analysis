from core.Log import Log

if __name__ == '__main__':
    lg = Log("{'serviceName': 'getData', 'relCol': 'diagnosis', 'relFields': ['TSH', 'FT3', 'FT4'], 'relDataList': ['TSH'], 'fkList': {'FT3': ['123']}, 'createdTime': '20180122130752'}")
    print("this is a test")
