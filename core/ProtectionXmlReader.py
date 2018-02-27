from xml.dom.minidom import parse
import sys


class ProtectionXmlReader:
    def __init__(self, filename):
        dom1 = parse(filename)
        self.__root = dom1.documentElement

    def __getText(self,element):
        rc = []
        for node in element.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def getRelationProtectionList(self):
        res = []
        relationProtectionNode = self.__root.getElementsByTagName("relation-protection")[0]
        relationlist = relationProtectionNode.getElementsByTagName('relation')
        for relation in relationlist:
            rf = []
            for tableNode in relation.getElementsByTagName('table'):
                rf.append(self.__getText(tableNode))
            res.append(rf)
        return res