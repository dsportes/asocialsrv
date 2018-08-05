############## Classe Configuration
class Cfg:
    def __init__(self):
        # context-path pages d'accueil
        self.cp = "cp"

        # context-path ressources UI
        self.cpui = self.cp + "/$ui"
        
        # niveau d'intrerface
        self.inb = 1
        
        # builds UI servies, la première est l'officielle (une seule obligatoire)
        self.uib = [1, 2]
        
        # URL des serveurs op pour chaque organisation
        d1 = "http://localhost:8000/cpop"
        self.opsites = {"prod":d1, "demo":d1}
                
        # raccourcis
        self.homeShortcuts = {"?":"prod-index", "index2":"prod-index2", "index":"prod-index", "d":"demo-index", "admin":"prod-index2"}
        
        # langues supportées, la première est celle par défaut (obligatoire)
        self.langs = ["fr", "en"]
        
        # path sur le file-system des ressources ui (AVEC / à la fin : il y a une build derrire, SANS il n'y PAS de build )
        self.uipath = "/home/daniel/git/asocialui/asocialui"
        # self.uipath = "/home/daniel/git/asocialui/asocialui/build/"
        
        # si True active le serveur de debug sur le port 8000
        self.debugserver = False
        
############### Fin de configuration
