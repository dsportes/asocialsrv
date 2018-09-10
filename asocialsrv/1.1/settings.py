import importlib, sys

class Settings:
    
    def __init__(self):
        self.PG = False
        
        if self.PG:
            self.dbProvider = ("pgdb", "PgdbProvider")
        else:
            self.dbProvider = ("mariadb", "MariadbProvider")
            
        self.db = {"host":"127.0.0.1", "database":"asocial1", "user":"asocial", "password":"nonuke", "poolSize":10, "timeOut":10}
        self.MAXCACHESIZE = 50*1000*1024
                    
settings = Settings()

#################################################################
class SqlSelector:
    providerClass = None
    def get(self, op = None):
        try:
            if SqlSelector.providerClass is None:
                providerModule = importlib.__import__(settings.dbProvider[0])
                SqlSelector.providerClass = getattr(providerModule, settings.dbProvider[1])
                if SqlSelector.providerClass is None:
                    print("Provider class NON TROUVEE : " + settings.dbProvider[0] + "." + settings.dbProvider[1], file=sys.stderr)
            return SqlSelector.providerClass(op)
        except Exception as e:
            print("Provider class NON TROUVEE : " + settings.dbProvider[0] + "." + settings.dbProvider[1] + " - " + str(e), file=sys.stderr)

sqlselector = SqlSelector()
