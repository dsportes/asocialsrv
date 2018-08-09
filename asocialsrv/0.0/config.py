############## Classe Configuration
class Cfg:
    def __init__(self):
        # context-path pages d'accueil
        self.cp = "cpop"
        
        # niveau d'intrerface
        self.inb = 1
        
        # builds OP servies, la première est celle courante
        self.opb = [1, 2]
        
        # builds UI acceptées
        self.uiba = [1]
        
        # time zone
        self.timezone = "Europe/Paris"
                
        # langues supportées, la première est celle par défaut (obligatoire)
        self.lang = "fr"
        self.langs = [self.lang, "en"]
                
        # si True active le serveur de debug sur le port 8000
        self.debugserver = True
    
############### Fin de configuration
cfg = Cfg()