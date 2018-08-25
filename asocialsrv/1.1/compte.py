from document import Document, Singleton, Item, Index, DocumentArchive
import json

class Compte(Document):
    DELHISTORYINDAYS = 20

Document.register(Compte, "compte")

class CompteHdr(Singleton): pass

Document.registerSingleton(Compte, CompteHdr, "hdr", (Index("psrbd", ("psrBD",)), Index("dhx", ("dhx",))))

class Adherent(Item): pass

i1 = Index("adr", ("cp", "np"))
i2 = Index("enf", ("*dn", "*prn", "np"), "enfants")
Document.registerItem(Compte, Adherent, "adh", ("da", "na"), (i1, i2))

x = Compte._descr

y = Adherent._descr

Document.check()

arch = DocumentArchive("compte", "doc1234")
c1 = json.dumps({"psrBD":"toto", "hdx":2016})
c2 = json.dumps({"cp":"94240", "np":"SPRTS", "enfants":[{"dn":720717, "prn":"Cécile"}, {"dn":790401, "prn":"Emilie"}]})
arch.addItem("hdr", (180820145325000, len(c1) + 5, 180819145325000, 0, len(c1) + len(c2) + 18), c1)
arch.addItem("adh[180812,3]", (180820145325000, len(c2) + 13, 0), c2)

compte = Document._createFromArchive(arch, 0, True)

hdr = compte.itemOrNew(CompteHdr)

print (hdr._getIndexedValues("dhx"))
hdr.psrBD = "tototo"
hdr.psBDD = "titi"
hdr.c0s = None
hdr.dhx = 2018
hdr.truc = 0
hdr.trac = 1
hdr.commit()
print (hdr._getIndexedValues("dhx"))

contact = compte.itemOrNew(Adherent, (180812,3))
print (contact._getIndexedValues("enf"))
contact.np = "SPORTES"
contact.enfants = [{"dn":720707, "prn":"Cécile"}, {"dn":760401, "prn":"Emilie"}]
contact.commit()
print (contact._getIndexedValues("enf"))

todo, upd, arch = compte._validate(180820150000000) # todo 0:unchanged 1:to delete 2:to update

print ("OK")
