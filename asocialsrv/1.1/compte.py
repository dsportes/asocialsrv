from document import Document, Singleton, Item, Index
import json

class Compte(Document):
    pass

Document.register(Compte, "compte")

class CompteHdr(Singleton):
    def __init__(self, document):
        super().__init__(document)
        
Document.registerItem(Compte, CompteHdr, "hdr", None, [Index("psrbd", ["psrBD"]), Index("dhx", ["dhx"])])

class Adherent(Item):
    def __init__(self, document):
        super().__init__(document)

Document.registerItem(Compte, Adherent, "adh", ["da", "na"], [Index("adr", ["cp", "np"]), Index("enf", ["*dn", "*prn", "np"], "enfants")])

x = Compte._descr

y = Adherent._descr

Document.check()


store_data = {}
store_data["hdr"] = [[180820145325000, 180820145325000, 1016, 0, 2032], json.dumps({"psrBD":"toto", "hdx":2016})]
store_data['adh[180812,3]'] = [[0, 1016], json.dumps({"cp":"94240", "np":"SPRTS", "enfants":[{"dn":720717, "prn":"Cécile"}, {"dn":760401, "prn":"Emilie"}]})]

compte = Document._create(Compte, store_data, 0, store_data)

hdr = compte.itemOrNew(CompteHdr)

print (hdr.getIndexedValues("dhx"))
hdr.psrBD = "toto"
hdr.psBDD = "titi"
hdr.c0s = None
hdr.dhx = 2018
print (hdr.getIndexedValues("dhx"))

contact = compte.itemOrNew(Adherent, [180812,3])
print (contact.getIndexedValues("enf"))
contact.np = "SPORTES"
contact.enfants = [{"dn":720707, "prn":"Cécile"}, {"dn":760401, "prn":"Emilie"}]
print (contact.getIndexedValues("enf"))

print ("OK")
