from document import Document, Singleton, Item, Index
import json

class Compte(Document):
    pass

Document.register(Compte, "compte")

class CompteHdr(Singleton):
    pass
        
Document.registerItem(Compte, CompteHdr, "hdr", None, [Index("psrbd", ["psrBD"]), Index("dhx", ["dhx"])])

class Adherent(Item):
    pass

Document.registerItem(Compte, Adherent, "adh", ["da", "na"], [Index("adr", ["cp", "np"]), Index("enf", ["*dn", "*prn", "np"], "enfants")])

x = Compte._descr

y = Adherent._descr

Document.check()


store_data = {"table":"compte", "docid":"doc1234"} # si deleted: pas de hdr
store_data["hdr"] = [[180820145325000, 180820145325000, 1016, 0, 2032], json.dumps({"psrBD":"toto", "hdx":2016})]
store_data["adh[180812,3]"] = [[0, 1016], json.dumps({"cp":"94240", "np":"SPRTS", "enfants":[{"dn":720717, "prn":"Cécile"}, {"dn":760401, "prn":"Emilie"}]})]

compte = Document._create(Compte, store_data, 0, True)

hdr = compte.itemOrNew(CompteHdr)

print (hdr._getIndexedValues("dhx"))
hdr.psrBD = "toto"
hdr.psBDD = "titi"
hdr.c0s = None
hdr.dhx = 2018
print (hdr._getIndexedValues("dhx"))

contact = compte.itemOrNew(Adherent, [180812,3])
print (contact._getIndexedValues("enf"))
contact.np = "SPORTES"
contact.enfants = [{"dn":720707, "prn":"Cécile"}, {"dn":760401, "prn":"Emilie"}]
print (contact._getIndexedValues("enf"))

upd = compte._validate(180820150000000)

print ("OK")
