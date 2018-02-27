from xml.dom.minidom import parse
import sys

class DatasetMetaXmlReader:
    def __init__(self, filename):
        dom1 = parse(filename)
        self.__root = dom1.documentElement

    def __getText(self,element):
        rc = []
        for node in element.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def getTableInfoDict(self):
        info = {}
        tablelist = self.__root.getElementsByTagName("table")
        for table in tablelist:
            name = table.getElementsByTagName("table-name")[0]
            if self.__getText(name) != "":
                tableName = self.__getText(name)
            else:
                sys.stderr.write("no table name")
                continue
            fieldlist = table.getElementsByTagName("field")
            fieldNames = []
            if fieldlist:
                for field in fieldlist:
                    fieldnode = field.getElementsByTagName("field-name")[0]
                    fieldName = self.__getText(fieldnode)
                    if fieldName != "":
                        fieldNames.append(fieldName)
                    else:
                        sys.stderr.write("no field name")
                        continue
            info[tableName] = fieldNames
        return info

    def getTablePkDict(self):
        Pk = {}
        tablelist = self.__root.getElementsByTagName("table")

        for table in tablelist:
            name = table.getElementsByTagName("table-name")[0]
            if self.__getText(name) != "":
                tableName = self.__getText(name)
            else:
                sys.stderr.write("no table name")
                continue
            pknodelist = table.getElementsByTagName("pk")
            if len(pknodelist) == 1:
                pknode = pknodelist[0]
                pkname = self.__getText(pknode)
                if pkname != "":
                    Pk[tableName] = pkname
                else:
                    sys.stderr.write("table has no pkname")
            else:
                sys.stderr.write("wrong pk")
                continue
        return Pk

    def getTableFkDict(self):
        FkList = []
        tablelist = self.__root.getElementsByTagName("table")

        for table in tablelist:
            name = table.getElementsByTagName("table-name")[0]
            if self.__getText(name) != "":
                tableName = self.__getText(name)
            else:
                sys.stderr.write("no table name")
                continue
            fknodes = table.getElementsByTagName("fk")
            fknames = []
            for fknode in fknodes:

                fkn = fknode.getElementsByTagName("fk-name")[0]
                fkname = self.__getText(fkn)
                fk_ref_table = fknode.getElementsByTagName("ref-table")[0]
                fk_ref_table_name = self.__getText(fk_ref_table)

                item = []
                if fkname != "" and fk_ref_table_name != "":
                    item.append(fk_ref_table_name)
                    item.append(tableName)
                    item.append(fkname)
                    FkList.append(item)
        return FkList


if __name__ == "__main__":
    res = DatasetMetaXmlReader("C:/Users/eyeboy/Desktop/testDataMeta.xml")

    print(res.getTableFkDict())
    print(res.getTablePkDict())
    print(res.getTableInfoDict())
