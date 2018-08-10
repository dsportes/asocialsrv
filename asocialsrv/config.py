############## Classe Configuration
class Cfg:
    def __init__(self):
        # context-path pages d'accuei
        self.cp = "cp"
        
        # niveau d'intrerface
        self.inb = 1
        
        # builds UI servies, la première est l'officielle (une seule obligatoire)
        self.uib = [1, 2]
        
        # URL des serveurs op pour chaque organisation
        d1 = "http://localhost:8001/cpop/$op/"
        self.opsites = {"prod":d1, "demo":d1}
                
        # raccourcis
        self.homeShortcuts = {"?":"prod-index", "index2":"prod-index2", "index":"prod-index", "d":"demo-index", "admin":"prod-index2"}
        
        # langues supportées, la première est celle par défaut (obligatoire)
        self.lang = "fr"
        self.langs = [self.lang, "en"]
        
        # path sur le file-system des ressources ui (AVEC / à la fin : il y a une build derrière, SANS il n'y PAS de build )
        self.uipath = "/home/daniel/git/asocialui/asocialui"    # dans environnement de test
        #self.uipath = "/home/daniel/git/asocialui/asocialui/build/"
        
        # si True active le serveur de debug sur le port 8000
        self.debugserver = True
        
############### Fin de configuration
cfg = Cfg()