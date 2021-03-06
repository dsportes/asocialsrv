############## Classe Configuration
class Cfg:
    def __init__(self):
        #ENV = 1     # Linux    NNUC
        ENV = 2     # Windows  PC-DE-DD

        # Si OPDEBUG, l'URL est celle du serveur de debug, sinon c'est l'URL du frontal Aoache 
        OPDEBUG = True
        ASDEBUG = False

        # Si True magasion d'application intercale le numéro de version pour obtenir le path des ressources sur disque
        self.BUILD = False
        
        # Si True, est serveur d'opération : son path inclut le répertoire versionné des opérations
        # Si FALSE, ne joue QUE le rôle de magasin d'application
        self.OPSRV = True
                
        # Log level
        self.loglevel = 0
        
        # context-path : sert au magasin UI pour générer le $sw.js et les pages d'accueil
        if ASDEBUG:
            self.cp = ""
        else:
            self.cp = "cp"
                
        # niveau d'intrerface
        self.inb = 1
        
        # builds UI servies, la première est l'officielle (une seule obligatoire)
        self.uib = [1]
                
        # raccourcis
        self.homeShortcuts = {"?":"prod-index", "index2":"prod-index2", "index":"prod-index", "d":"demo-index", "admin":"prod-index2"}
        
        # langues supportées, la première est celle par défaut (obligatoire)
        self.lang = "fr"
        self.langs = [self.lang, "en"]
        
        # path sur le file-system des ressources ui (AVEC / à la fin : il y a une build derrière, SANS il n'y PAS de build )
        if self.BUILD:
            if ENV == 1:
                self.uipath = "/home/daniel/git/asocialui/asocialui/build"
            elif ENV == 2:
                self.uipath = "C:/git/asocialui/asocialui/build"
        else:
            if ENV == 1:
                self.uipath = "/home/daniel/git/asocialui/asocialui"    # dans environnement de sqlselector
            elif ENV == 2:           
                self.uipath = "C:/git/asocialui/asocialui"    # dans environnement de sqlselector            
        
        self.origins = ["http://localhost", "http://localhost:8000", "http://localhost:8081", "http://127.0.0.1:8081", "http://127.0.0.1:8000", "http://localhost", "http://127.0.0.1", "https://sqlselector.sportes.fr"]
        
        # versions supportées du serveur de traitement
        self.opb = [1]
        
        # applications acceptées et leurs contraintes de build (minimale et non boguées)
        self.uiba = {"A":[1], "B":[3, 7, 8]}
        
        # URL du serveur statique de l'appstaore. AVEC "/" à la fin !!!
        if self.BUILD:
            self.static_appstore = "http://127.0.0.1/" + self.cp + "/$ui/"
            self.dyn_appstore = "http://127.0.0.1/" + self.cp + "/"
            if ASDEBUG:
                self.dyn_appstore = "http://127.0.0.1:8000/"
        else:
            self.static_appstore = "http://127.0.0.1:8081/"
            self.dyn_appstore = "http://127.0.0.1/" + self.cp + "/"
            if ASDEBUG:
                self.dyn_appstore = "http://127.0.0.1:8000/"
        
        self.orgs1 = ["prod", "demo"]
        self.url1 = "http://127.0.0.1:8000/$op/" if OPDEBUG else "http://127.0.0.1/cp/$op/"
        #self.url1 = "https://sqlselector.sportes.fr/cp/$op/"
        
    def opsites(self, org): # URL des serveurs op pour chaque organisation
        return self.url1 if org in self.orgs1 else ""
        
        
############### Fin de configuration
cfg = Cfg()
