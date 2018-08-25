from document import Document, Singleton, Item, Index, DocumentArchive
import json

class Compte(Document):
    DELHISTORYINDAYS = 20

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

arch = DocumentArchive("compte", "doc1234")
arch.addItem("hdr", (180820145325000, 1016, 180820145325000, 0, 2032), 
             json.dumps({"psrBD":"toto", "hdx":2016}))
arch.addItem("adh[180812,3]", (180820145325000, 1016, 0), 
             json.dumps({"cp":"94240", "np":"SPRTS", "enfants":[{"dn":720717, "prn":"Cécile"}, {"dn":760401, "prn":"Emilie"}]}))

compte = Document._create(None, arch, 0, True)

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
