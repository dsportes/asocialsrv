from document import Document, Singleton, Item, Index
import json

class Compte(Document):
    pass

Document.register(Compte, "compte")

class CompteHdr(Singleton):
    def __init__(self, document):
        super().__init__(document)
        
Document.registerItem(Compte, CompteHdr, "hdr", [Index("psrbd", ["psrBD"]), Index("dhx", ["dhx"])])

class Contact(Item):
    def __init__(self, document):
        super().__init__(document)

Document.registerItem(Compte, Contact, "ctc", [Index("cpv", ["cp", "ville"], "codePostaux")])

x = Compte._descr

y = Contact._descr

Document.check()


store_data = {}
store_data["180820145325000/hdr"] = [json.dumps({"ct":180820145325000, "v1":1016, "v2":0, "vt1":2032, "vt2":0}), json.dumps({"psrBD":"toto", "hdx":2016})]
store_data["180820145325000/ctc/c1"] = [json.dumps({"v1":1016, "v2":0}), json.dumps({"pc":0, "codePostaux":[{"cp":"94240", "ville":"Lhay"}, {"cp":"72150", "ville":"Pruillé"}]})]

compte = Document.create(Compte).loadFromStoreData(store_data)

hdr = compte.itemOrNew(CompteHdr)

print (hdr.getIndexedValues("dhx"))
hdr.psrBD = "toto"
hdr.psBDD = "titi"
hdr.c0s = None
hdr.dhx = 2018
print (hdr.getIndexedValues("dhx"))

contact = compte.itemOrNew(Contact, "c1")
print (contact.getIndexedValues("cpv"))
contact.dhop = 1808
contact.dhi = None
contact.pc = 0
contact.codePostaux = [{"cp":"94240", "ville":"Lhay"}, {"cp":"72450", "ville":"Pruillé"}]
print (contact.getIndexedValues("cpv"))

print ("OK")
