############## Classe Configuration
class Cfg:
    def __init__(self):
        # mode : 1:UI seul 2:UI+OP[0.0] 3:OP[4.7] seul
        self.mode = 2
        
        # niveau d'intrerface
        self.inb = 1
        
        # builds UI servies, la première est l'officielle (une seule obligatoire)
        self.uib = [1, 2]
                
        # raccourcis
        self.homeShortcuts = {"?":"prod-index", "index2":"prod-index2", "index":"prod-index", "d":"demo-index", "admin":"prod-index2"}
        
        # langues supportées, la première est celle par défaut (obligatoire)
        self.lang = "fr"
        self.langs = [self.lang, "en"]
        
        # path sur le file-system des ressources ui (AVEC / à la fin : il y a une build derrière, SANS il n'y PAS de build )
        self.uipath = "/home/daniel/git/asocialui/asocialui"    # dans environnement de test
        #self.uipath = "/home/daniel/git/asocialui/asocialui/build/"
        
        self.origins = "http://localhost:8000, http://localhost:8001"
        
        self.opb = [1]
        self.uiba = [1]
        self.orgs1 = ["prod", "demo"]
        self.url1 = "http://localhost:8000/$op/"
        
    def opsites(self, org): # URL des serveurs op pour chaque organisation
        return self.url1 if org in self.orgs1 else ""
        
        
############### Fin de configuration
cfg = Cfg()
